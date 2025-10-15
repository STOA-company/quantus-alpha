import os
import signal
import requests
from datetime import datetime

# Gunicorn 설정
bind = f"0.0.0.0:{os.getenv('PORT', '8000')}"
workers = 8
worker_class = "uvicorn.workers.UvicornWorker"
timeout = 90
keepalive = 2
max_requests = 1000
max_requests_jitter = 100

# 로깅 설정
accesslog = "-"
errorlog = "-"
loglevel = "info"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Slack 설정 - 새로운 webhook URL 사용
ENV = os.getenv("ENV", "dev")
webhook_url = "https://hooks.slack.com/services/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"

# 전역 변수로 현재 처리 중인 요청 추적
current_requests = {}


def send_slack_message(message: str, color: str = None):
    """Slack에 직접 POST 요청을 보냅니다."""
    if not webhook_url:
        print("Slack webhook URL이 설정되지 않았습니다.")
        return False
        
    try:
        payload = {
            "text": message,
            "username": "Gunicorn Server",
            "icon_emoji": ":rocket:"
        }
        
        if color:
            payload["attachments"] = [{
                "color": color,
                "text": message
            }]
        
        # 디버깅을 위한 정보 출력
        print(f"Slack webhook URL: {webhook_url}")
        print(f"Payload: {payload}")
        
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Gunicorn-Server/1.0'
        }
        
        response = requests.post(
            webhook_url,
            json=payload,
            headers=headers,
            timeout=10
        )
        
        print(f"Response status: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")
        print(f"Response text: {response.text}")
        
        response.raise_for_status()
        print("Slack 알림이 성공적으로 전송되었습니다.")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"Slack 알림 전송 실패: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response text: {e.response.text}")
        return False
    except Exception as e:
        print(f"Slack 알림 전송 중 예상치 못한 오류: {e}")
        return False


def when_ready(_server):
    """서버가 시작될 때 호출"""
    print(f"🚀 Gunicorn server started with {workers} workers on {bind}")
    try:
        message = (
            f"🚀 **Gunicorn 서버 시작**\n\n"
            f"*환경*: {ENV}\n"
            f"*워커 수*: {workers}\n"
            f"*바인드*: {bind}\n"
            f"*타임아웃*: {timeout}초\n"
            f"*시작 시간*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        # result = send_slack_message(message, color="#36a64f")
        print(f"Slack notification result: {message}")
    except Exception as e:
        print(f"Failed to send startup notification: {e}")
        import traceback
        traceback.print_exc()


def worker_timeout(worker):
    """워커 타임아웃 발생 시 호출"""
    worker_pid = worker.pid
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"❌❌❌ WORKER_TIMEOUT CALLBACK CALLED: Worker {worker_pid} timed out at {current_time} ❌❌❌")
    
    try:
        message = (
            f"❌ **WORKER TIMEOUT 발생**\n\n"
            f"*환경*: {ENV}\n"
            f"*워커 PID*: {worker_pid}\n"
            f"*발생 시간*: {current_time}\n"
            f"*타임아웃*: {timeout}초\n\n"
            f"🔄 워커가 재시작됩니다."
        )
        
        # result = send_slack_message(message, color="#ff0000")
        print(f"✅ Slack notification result: {message} for worker {worker_pid}")
    except Exception as e:
        print(f"❌ Failed to send worker timeout notification: {e}")
        # 예외 상세 정보도 출력
        import traceback
        traceback.print_exc()


def worker_exit(_server, worker):
    """워커가 종료될 때 호출"""
    worker_pid = worker.pid
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"⚠️  WORKER EXIT: Worker {worker_pid} exited at {current_time}")
    
    # 현재 요청 정보 정리
    if worker_pid in current_requests:
        del current_requests[worker_pid]


def worker_abort(worker):
    """워커가 비정상 종료될 때 호출"""
    worker_pid = worker.pid
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"🚨 WORKER ABORT: Worker {worker_pid} aborted at {current_time}")
    
    try:
        request_info = current_requests.get(worker_pid, "Unknown request")
        
        message = (
            f"🚨 **WORKER ABORT 발생**\n\n"
            f"*환경*: {ENV}\n"
            f"*워커 PID*: {worker_pid}\n"
            f"*발생 시간*: {current_time}\n"
            f"*현재 처리 중인 요청*: {request_info}\n\n"
            f"💀 워커가 비정상 종료되었습니다."
        )
        
        # send_slack_message(message)
    except Exception as e:
        print(f"Failed to send worker abort notification: {e}")
    
    # 현재 요청 정보 정리
    if worker_pid in current_requests:
        del current_requests[worker_pid]


def on_exit(_server):
    """서버가 종료될 때 호출"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"🛑 Gunicorn server shutting down at {current_time}")
    
    try:
        message = (
            f"🛑 **Gunicorn 서버 종료**\n\n"
            f"*환경*: {ENV}\n"
            f"*종료 시간*: {current_time}"
        )
        # send_slack_message(message)
    except Exception as e:
        print(f"Failed to send shutdown notification: {e}")


# 프로세스 관리 설정
preload_app = True
worker_connections = 1000

# 워커 타임아웃 감지를 위한 추가 설정
graceful_timeout = 30
worker_tmp_dir = "/dev/shm"  # 메모리 기반 임시 디렉토리

# 신호 처리 개선
def handle_worker_signal(signum, _frame):
    """워커에서 시그널 처리"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    worker_pid = os.getpid()
    signal_name = signal.Signals(signum).name
    
    print(f"📡 Worker {worker_pid} received signal {signal_name} at {current_time}")
    
    if signum == signal.SIGTERM:
        try:
            message = (
                f"📡 **Worker Signal 받음**\n\n"
                f"*환경*: {ENV}\n"
                f"*워커 PID*: {worker_pid}\n"
                f"*시그널*: {signal_name}\n"
                f"*시간*: {current_time}"
            )
            # send_slack_message(message)
        except Exception as e:
            print(f"Failed to send signal notification: {e}")


# 워커에서 시그널 핸들러 등록
def post_worker_init(worker):
    """워커 초기화 후 호출"""
    signal.signal(signal.SIGTERM, handle_worker_signal)
    signal.signal(signal.SIGINT, handle_worker_signal)
    
    worker_pid = worker.pid
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"✅ Worker {worker_pid} initialized at {current_time}")