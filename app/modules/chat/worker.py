# app/chat/worker.py
import os
import time
import json
from celery import Celery
from typing import Dict, Any
from redis import Redis

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")

redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)

if REDIS_PASSWORD:
    REDIS_URL = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/0"
else:
    REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"

celery_app = Celery("chat_worker", broker=REDIS_URL, backend=REDIS_URL)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Seoul",
    enable_utc=True,
    task_routes={"app.modules.chat.worker.*": {"queue": "chat_queue"}},
)


def save_task_to_store(task_id: str, task_data: Dict):
    redis_client.set(f"task:{task_id}", json.dumps(task_data))


@celery_app.task(name="app.chat.worker.process_chat")
def process_chat(message: str) -> Dict[str, Any]:
    task_id = process_chat.request.id

    try:
        response_text = "LLM API 연동이 아직 구현되지 않았습니다."

        # 응답 데이터 구성
        response_data = {
            "task_id": task_id,
            "status": "SUCCESS",
            "request": message,
            "response": response_text,
            "timestamp": time.time(),
        }

        save_task_to_store(task_id, response_data)

        return response_data
    except Exception as e:
        error_data = {"task_id": task_id, "status": "ERROR", "error": str(e), "timestamp": time.time()}
        save_task_to_store(task_id, error_data)
        return error_data
