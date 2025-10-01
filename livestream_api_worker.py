import datetime
import uuid
import subprocess
import os
import sys
import logging
import time
import threading
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_socketio import SocketIO
import psutil

# ==============================================================================
# CẤU HÌNH LOGGING VÀ MÔI TRƯỜNG
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
VIDEO_DIRECTORY = os.getenv('VIDEO_DIRECTORY', '/videos')
logging.info(f"Thư mục video được cấu hình: {VIDEO_DIRECTORY}")

# ==============================================================================
# CẤU HÌNH FLASK VÀ WEBSOCKET
# ==============================================================================
app = Flask(__name__)
CORS(app)
# Sử dụng gevent để có hiệu suất tốt nhất cho các tác vụ bất đồng bộ
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent')

# ==============================================================================
# BIẾN TOÀN CỤC VÀ KHÓA (LOCK)
# ==============================================================================
schedules = []
running_streams = {}
# Rất quan trọng: Sử dụng Lock để tránh race condition khi nhiều luồng
# cùng truy cập vào các biến schedules và running_streams
state_lock = threading.Lock()
background_thread = None

# ==============================================================================
# FFMPEG COMMANDS
# ==============================================================================
FFMPEG_CMD_INFINITE = (
    'ffmpeg -re -i "{video_file}" -c:v copy -c:a copy '
    '-f flv -nostdin "{rtmp_url}"'
)
FFMPEG_CMD_TIMED = (
    'ffmpeg -re -stream_loop -1 -i "{video_file}" -t {duration} '
    '-c:v copy -c:a copy -f flv -nostdin "{rtmp_url}"'
)

# ==============================================================================
# CÁC HÀM TIỆN ÍCH (HELPER FUNCTIONS)
# ==============================================================================
def _async_terminate_worker(pid):
    try:
        logging.info(f"Luồng nền bắt đầu dừng PID {pid}.")
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        for child in children:
            child.terminate()
        parent.terminate()
        gone, alive = psutil.wait_procs(children + [parent], timeout=5)
        for p in alive:
            p.kill()
        logging.info(f"Đã dọn dẹp xong cho PID {pid}.")
    except psutil.NoSuchProcess:
        pass # Không cần log warning vì tiến trình có thể đã dừng trước đó
    except Exception:
        logging.exception(f"Lỗi trong luồng nền khi dừng PID {pid}.")

def terminate_process_async(pid):
    thread = threading.Thread(target=_async_terminate_worker, args=(pid,))
    thread.daemon = True
    thread.start()

def get_schedule_status(schedule):
    # (Hàm này giữ nguyên như trước)
    broadcast_time_str = schedule['broadcastDateTime']
    try:
        broadcast_time = datetime.datetime.fromisoformat(broadcast_time_str.replace('Z', '+00:00'))
        current_time = datetime.datetime.now(datetime.timezone.utc)
        if schedule['status'] in ['FAILED', 'COMPLETED', 'STOPPING']:
            return schedule['status']
        if broadcast_time > current_time:
            return "PENDING"
        duration_minutes = schedule.get('durationMinutes')
        if duration_minutes:
            end_time = broadcast_time + datetime.timedelta(minutes=duration_minutes)
            if current_time >= end_time:
                return "COMPLETED"
        return "LIVE"
    except Exception:
        return "UNKNOWN"

def is_process_running(process):
    if not process: return False
    try:
        p = psutil.Process(process.pid)
        return p.is_running() and p.status() not in [psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD]
    except psutil.NoSuchProcess:
        return False
    return True

# ==============================================================================
# LOGIC QUẢN LÝ VÀ GIÁM SÁT TRONG LUỒNG NỀN
# ==============================================================================
def manage_ffmpeg_stream(schedule):
    schedule_id = schedule['id']
    target_status = get_schedule_status(schedule)

    if schedule['status'] != target_status:
        schedule['status'] = target_status
    
    process = running_streams.get(schedule_id)

    if is_process_running(process):
        if target_status != 'LIVE':
            logging.info(f"Luồng ID={schedule_id} đã hết giờ hoặc bị dừng. Dừng tiến trình...")
            terminate_process_async(process.pid)
            del running_streams[schedule_id]
    else:
        if schedule_id in running_streams:
             del running_streams[schedule_id]
             if target_status == 'LIVE' and not schedule.get('durationMinutes'):
                 logging.info(f"TỰ ĐỘNG KHỞI ĐỘNG LẠI luồng VÔ HẠN ID={schedule_id}.")
             else:
                 schedule['status'] = 'COMPLETED'

    if schedule['status'] == 'LIVE' and schedule_id not in running_streams:
        video_filename = schedule['videoIdentifier']
        video_file_path = os.path.join(VIDEO_DIRECTORY, video_filename)
        if not os.path.isfile(video_file_path):
            schedule['status'] = 'FAILED'
            logging.error(f"LỖI: File video không tồn tại: {video_file_path}")
            return

        rtmp_url = f"{schedule['rtmpServer']}/{schedule['streamKey']}"
        duration_minutes = schedule.get('durationMinutes')
        
        command = FFMPEG_CMD_TIMED.format(video_file=video_file_path, rtmp_url=rtmp_url, duration=duration_minutes * 60) if duration_minutes else FFMPEG_CMD_INFINITE.format(video_file=video_file_path, rtmp_url=rtmp_url)
        logging.info(f"KHỞI ĐỘNG luồng ID={schedule_id}.")
        
        try:
            new_process = subprocess.Popen(command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            running_streams[schedule_id] = new_process
        except Exception:
            logging.exception(f"LỖI NGHIÊM TRỌNG khi chạy ffmpeg cho ID={schedule_id}.")
            schedule['status'] = 'FAILED'

def background_monitor():
    """Tác vụ chạy nền để giám sát và cập nhật trạng thái."""
    logging.info("Bắt đầu luồng giám sát nền...")
    while True:
        # Sao chép trạng thái để tránh block quá lâu
        with state_lock:
            schedules_copy = list(schedules)
        
        # Xử lý logic bên ngoài lock
        for schedule in schedules_copy:
            manage_ffmpeg_stream(schedule)

        # Khóa lại chỉ để cập nhật
        with state_lock:
            # Gửi trạng thái mới nhất cho tất cả các client
            socketio.emit('broadcast_update', schedules)

        socketio.sleep(2) # Quan trọng: dùng socketio.sleep để tương thích với gevent

# ==============================================================================
# ĐỊNH NGHĨA CÁC SỰ KIỆN WEBSOCKET
# ==============================================================================

@socketio.on('connect')
def handle_connect():
    """Khi một client kết nối, bắt đầu luồng nền nếu chưa có và gửi dữ liệu."""
    global background_thread
    with state_lock:
        if background_thread is None:
            background_thread = socketio.start_background_task(target=background_monitor)
        # Gửi toàn bộ danh sách lịch trình cho client vừa kết nối
        socketio.emit('broadcast_update', schedules, room=request.sid)
    logging.info(f"Client {request.sid} đã kết nối.")

@socketio.on('create_schedule')
def handle_create_schedule(data):
    logging.info(f"Nhận yêu cầu tạo lịch trình: {data['title']}")
    with state_lock:
        new_schedule = {
            "id": str(uuid.uuid4()), "title": data['title'], "videoIdentifier": data['videoIdentifier'],
            "broadcastDateTime": data['broadcastDateTime'], "rtmpServer": data.get('rtmpServer'),
            "streamKey": data['streamKey'], "durationMinutes": data.get('durationMinutes'), "status": "PENDING"
        }
        schedules.append(new_schedule)
    # Không cần emit ở đây, luồng nền sẽ tự động phát hiện và broadcast
    
@socketio.on('stop_schedule')
def handle_stop_schedule(data):
    schedule_id = data.get('id')
    logging.info(f"Nhận yêu cầu dừng thủ công luồng ID={schedule_id}")
    with state_lock:
        schedule = next((s for s in schedules if s['id'] == schedule_id), None)
        if schedule:
            schedule['status'] = 'STOPPING' # Trạng thái trung gian
            if schedule_id in running_streams:
                pid = running_streams[schedule_id].pid
                terminate_process_async(pid)
    # Luồng nền sẽ dọn dẹp và cập nhật trạng thái cuối cùng là COMPLETED

@socketio.on('delete_schedule')
def handle_delete_schedule(data):
    schedule_id = data.get('id')
    logging.info(f"Nhận yêu cầu xóa luồng ID={schedule_id}")
    with state_lock:
        if schedule_id in running_streams:
            pid = running_streams[schedule_id].pid
            terminate_process_async(pid)
            del running_streams[schedule_id]
        # Xóa lịch trình khỏi danh sách
        schedules[:] = [s for s in schedules if s['id'] != schedule_id]

# ==============================================================================
# ĐIỂM KHỞI CHẠY
# ==============================================================================
if __name__ == '__main__':
    logging.info("--- KHỞI ĐỘNG SERVER FLASK VỚI SOCKET.IO ---")
    socketio.run(app, host='0.0.0.0', port=5000)