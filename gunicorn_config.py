import os
import signal
from datetime import datetime

from app.core.extra.SlackNotifier import SlackNotifier

# Gunicorn 설정
bind = f"0.0.0.0:{os.getenv('PORT', '8000')}"
workers = 4
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

# Slack 설정
ENV = os.getenv("ENV", "dev")
if ENV == "stage":
    webhook_url = "https://hooks.slack.com/services/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"
else:
    webhook_url = "https://hooks.slack.com/services/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"

slack_notifier = SlackNotifier(webhook_url=webhook_url)

# 전역 변수로 현재 처리 중인 요청 추적
current_requests = {}


def when_ready(_server):
    """서버가 시작될 때 호출"""
    print(f"🚀 Gunicorn server started with {workers} workers on {bind}")
    try:
        slack_notifier.notify_error(
            f"🚀 **Gunicorn 서버 시작**\n\n"
            f"*환경*: {ENV}\n"
            f"*워커 수*: {workers}\n"
            f"*바인드*: {bind}\n"
            f"*타임아웃*: {timeout}초\n"
            f"*시작 시간*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
    except Exception as e:
        print(f"Failed to send startup notification: {e}")


def worker_timeout(worker):
    """워커 타임아웃 발생 시 호출"""
    worker_pid = worker.pid
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"❌ WORKER TIMEOUT: Worker {worker_pid} timed out at {current_time}")
    
    try:
        # 현재 처리 중인 요청 정보 가져오기
        request_info = current_requests.get(worker_pid, "Unknown request")
        
        message = (
            f"❌ **WORKER TIMEOUT 발생**\n\n"
            f"*환경*: {ENV}\n"
            f"*워커 PID*: {worker_pid}\n"
            f"*발생 시간*: {current_time}\n"
            f"*타임아웃*: {timeout}초\n"
            f"*현재 처리 중인 요청*: {request_info}\n\n"
            f"🔄 워커가 재시작됩니다."
        )
        
        slack_notifier.notify_error(message)
    except Exception as e:
        print(f"Failed to send worker timeout notification: {e}")


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
        
        slack_notifier.notify_error(message)
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
        slack_notifier.notify_error(
            f"🛑 **Gunicorn 서버 종료**\n\n"
            f"*환경*: {ENV}\n"
            f"*종료 시간*: {current_time}"
        )
    except Exception as e:
        print(f"Failed to send shutdown notification: {e}")


# 프로세스 관리 설정
preload_app = True
worker_connections = 1000

# 신호 처리 개선
def handle_worker_signal(signum, _frame):
    """워커에서 시그널 처리"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    worker_pid = os.getpid()
    signal_name = signal.Signals(signum).name
    
    print(f"📡 Worker {worker_pid} received signal {signal_name} at {current_time}")
    
    if signum == signal.SIGTERM:
        try:
            slack_notifier.notify_error(
                f"📡 **Worker Signal 받음**\n\n"
                f"*환경*: {ENV}\n"
                f"*워커 PID*: {worker_pid}\n"
                f"*시그널*: {signal_name}\n"
                f"*시간*: {current_time}"
            )
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