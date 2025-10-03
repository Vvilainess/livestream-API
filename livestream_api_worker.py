import datetime
import uuid
import subprocess
import os
import sys
import logging
import time
import threading
from flask import Flask
from flask_cors import CORS
from flask_socketio import SocketIO
import psutil

# ==============================================================================
# CẤU HÌNH LOGGING VÀ MÔI TRƯỜNG
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
VIDEO_DIRECTORY = os.getenv('VIDEO_DIRECTORY', '/videos')
logging.info(f"Thư mục video được cấu hình: {VIDEO_DIRECTORY}")

# ==============================================================================
# CẤU HÌNH FLASK VÀ WEBSOCKET
# ==============================================================================
app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent')

# ==============================================================================
# BIẾN TOÀN CỤC VÀ KHÓA (LOCK)
# ==============================================================================
schedules = []
running_streams = {}
retry_start_times = {} 
state_lock = threading.Lock()
background_thread = None
RETRY_WINDOW_SECONDS = 180  # 3 phút

# ==============================================================================
# FFMPEG COMMANDS (ĐÃ TỐI ƯU HÓA)
# ==============================================================================
FFMPEG_BASE_OPTIONS = "-loglevel error -hide_banner"
FFMPEG_CMD_INFINITE = (
    f'ffmpeg -re -stream_loop -1 {FFMPEG_BASE_OPTIONS} '
    '-i "{video_file}" -c:v copy -c:a copy -f flv '
    '-nostdin "{rtmp_url}"'
)
FFMPEG_CMD_TIMED = (
    f'ffmpeg -re -stream_loop -1 {FFMPEG_BASE_OPTIONS} '
    '-i "{video_file}" -t {duration} '
    '-c:v copy -c:a copy -f flv '
    '-nostdin "{rtmp_url}"'
)

# ==============================================================================
# CÁC HÀM TIỆN ÍCH (HELPER FUNCTIONS)
# ==============================================================================
def force_kill_process(pid):
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        all_procs = children + [parent]
        for proc in all_procs:
            try: proc.terminate()
            except psutil.NoSuchProcess: pass
        _, alive = psutil.wait_procs(all_procs, timeout=3)
        for proc in alive:
            try: proc.kill()
            except psutil.NoSuchProcess: pass
    except psutil.NoSuchProcess:
        pass

def get_schedule_status(schedule):
    # (Hàm này giữ nguyên như trước)
    broadcast_time_str = schedule['broadcastDateTime']
    try:
        broadcast_time = datetime.datetime.fromisoformat(broadcast_time_str.replace('Z', '+00:00'))
        current_time = datetime.datetime.now(datetime.timezone.utc)
        if schedule['status'] in ['FAILED', 'COMPLETED', 'STOPPING', 'RETRYING']:
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
        return p.is_running() and p.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False
    return True

# ==============================================================================
# LOGIC QUẢN LÝ VÀ GIÁM SÁT TRONG LUỒNG NỀN
# ==============================================================================
def manage_ffmpeg_stream(schedule):
    schedule_id = schedule['id']
    target_status = get_schedule_status(schedule)
    
    if schedule['status'] != 'RETRYING' and schedule['status'] != target_status:
        schedule['status'] = target_status
    
    process = running_streams.get(schedule_id)

    if is_process_running(process):
        if schedule_id in retry_start_times:
            logging.info(f"Luồng ID={schedule_id} đã phục hồi thành công. Hủy chu kỳ retry.")
            retry_start_times.pop(schedule_id, None)
        if target_status != 'LIVE':
            logging.info(f"Luồng ID={schedule_id} đã hết giờ. Dừng process...")
            force_kill_process(process.pid)
            del running_streams[schedule_id]
        return

    if schedule_id in running_streams:
        del running_streams[schedule_id]

    if target_status != 'LIVE':
        return

    first_failure_time = retry_start_times.get(schedule_id)
    if first_failure_time:
        elapsed_time = time.time() - first_failure_time
        if elapsed_time > RETRY_WINDOW_SECONDS:
            schedule['status'] = 'FAILED'
            logging.error(f"Luồng ID={schedule_id} FAILED. Đã ngừng thử lại sau {RETRY_WINDOW_SECONDS} giây.")
            retry_start_times.pop(schedule_id, None)
            return
        else:
            schedule['status'] = 'RETRYING'
            remaining_time = int(RETRY_WINDOW_SECONDS - elapsed_time)
            logging.warning(f"Thử lại luồng ID={schedule_id}. Còn lại {remaining_time} giây trong chu kỳ retry.")
    else:
        # Kiểm tra file chỉ ở lần khởi động ĐẦU TIÊN
        video_filename = schedule['videoIdentifier']
        video_file_path = os.path.join(VIDEO_DIRECTORY, video_filename)
        if not os.path.isfile(video_file_path):
            schedule['status'] = 'FAILED'
            logging.error(f"LỖI: File video không tồn tại: {video_file_path}. Luồng sẽ không bắt đầu.")
            return

        logging.warning(f"Luồng ID={schedule_id} gặp sự cố. Bắt đầu chu kỳ thử lại trong {RETRY_WINDOW_SECONDS} giây.")
        retry_start_times[schedule_id] = time.time()
        schedule['status'] = 'RETRYING'
    
    video_filename = schedule['videoIdentifier']
    video_file_path = os.path.join(VIDEO_DIRECTORY, video_filename)
    rtmp_url = f"{schedule['rtmpServer']}/{schedule['streamKey']}"
    duration_minutes = schedule.get('durationMinutes')
    command = FFMPEG_CMD_TIMED.format(video_file=video_file_path, rtmp_url=rtmp_url, duration=duration_minutes * 60) if duration_minutes else FFMPEG_CMD_INFINITE.format(video_file=video_file_path, rtmp_url=rtmp_url)
    
    logging.info(f"KHỞI ĐỘNG luồng ID={schedule_id}...")
    try:
        popen_kwargs = {
            'shell': True, 'stdout': subprocess.DEVNULL, 'stderr': subprocess.DEVNULL, 'universal_newlines': True
        }
        if os.name != 'nt':
            popen_kwargs['preexec_fn'] = os.setsid
        new_process = subprocess.Popen(command, **popen_kwargs)

        # def log_stderr_thread():
            # try:
                # for line in iter(new_process.stderr.readline, ''):
                    # if line: logging.error(f"[FFMPEG-ERROR ID={schedule_id}] {line.strip()}")
            # except ValueError: pass
        
        # threading.Thread(target=log_stderr_thread, daemon=True).start()
        running_streams[schedule_id] = new_process
        socketio.sleep(1)

        if not is_process_running(new_process):
            logging.error(f"LỖI NGAY LẬP TỨC: Tiến trình ffmpeg cho ID={schedule_id} đã tắt.")
            del running_streams[schedule_id]
            return

        schedule['status'] = 'LIVE'
        logging.info(f"FFmpeg khởi động thành công cho ID={schedule_id} (PID: {new_process.pid})")
        
    except Exception as e:
        logging.exception(f"LỖI NGHIÊM TRỌNG khi chạy Popen cho ID={schedule_id}: {e}")
        schedule['status'] = 'FAILED'

# ==============================================================================
# HÀM GIÁM SÁT NỀN VÀ CÁC SỰ KIỆN WEBSOCKET
# ==============================================================================
def background_monitor():
    logging.info("Bắt đầu luồng giám sát nền...")
    while True:
        with state_lock:
            for schedule in schedules:
                manage_ffmpeg_stream(schedule)
            socketio.emit('broadcast_update', schedules)
        socketio.sleep(2)

@socketio.on('connect')
def handle_connect():
    global background_thread
    with state_lock:
        if background_thread is None:
            background_thread = socketio.start_background_task(target=background_monitor)
        socketio.emit('broadcast_update', schedules)

# [THAY ĐỔI] Sử dụng Acknowledgement để trả về kết quả
@socketio.on('create_schedule')
def handle_create_schedule(data):
    video_filename = data.get('videoIdentifier')
    video_file_path = os.path.join(VIDEO_DIRECTORY, video_filename)

    # Bước 1: Xác thực thông tin
    if not os.path.isfile(video_file_path):
        logging.error(f"Xác thực thất bại: File '{video_filename}' không tồn tại.")
        # Trả về lỗi cho client
        return {'success': False, 'error': f"File video '{video_filename}' không tồn tại trong thư mục /videos."}

    # Bước 2: Nếu thông tin hợp lệ, tạo lịch trình
    with state_lock:
        new_schedule = {
            "id": str(uuid.uuid4()), "title": data['title'], "videoIdentifier": data['videoIdentifier'],
            "broadcastDateTime": data['broadcastDateTime'], "rtmpServer": data.get('rtmpServer'),
            "streamKey": data['streamKey'], "durationMinutes": data.get('durationMinutes'), "status": "PENDING"
        }
        schedules.append(new_schedule)
        logging.info(f"Đã tạo lịch trình ID={new_schedule['id']} sau khi xác thực thành công.")
    
    # Trả về thành công cho client
    return {'success': True, 'schedule': new_schedule}


@socketio.on('stop_schedule')
def handle_stop_schedule(data):
    schedule_id = data.get('id')
    with state_lock:
        schedule = next((s for s in schedules if s['id'] == schedule_id), None)
        if schedule:
            schedule['status'] = 'STOPPING'
            if schedule_id in running_streams:
                pid = running_streams[schedule_id].pid
                force_kill_process(pid)
                del running_streams[schedule_id]
            retry_start_times.pop(schedule_id, None)
            schedule['status'] = 'COMPLETED'
            socketio.emit('broadcast_update', schedules)

@socketio.on('delete_schedule')
def handle_delete_schedule(data):
    schedule_id = data.get('id')
    with state_lock:
        if schedule_id in running_streams:
            pid = running_streams[schedule_id].pid
            force_kill_process(pid)
            del running_streams[schedule_id]
        retry_start_times.pop(schedule_id, None)
        schedules[:] = [s for s in schedules if s['id'] != schedule_id]
        socketio.emit('broadcast_update', schedules)

# ==============================================================================
# ĐIỂM KHỞI CHẠY
# ==============================================================================
if __name__ == '__main__':
    logging.info("--- KHỞI ĐỘNG SERVER FLASK VỚI SOCKET.IO ---")
    socketio.run(app, host='0.0.0.0', port=5000)
