# app/chat/worker.py
import json
import os
import time
from typing import Any, Dict

from celery import Celery
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


@celery_app.task(name="app.modules.chat.worker.process_chat")
def process_chat(message: str) -> Dict[str, Any]:
    task_id = process_chat.request.id

    try:
        initial_state = {
            "task_id": task_id,
            "status": "PROCESSING",
            "request": message,
            "response": "",
            "chunks_received": 0,
            "timestamp": time.time(),
        }
        save_task_to_store(task_id, initial_state)

        chunks = [
            "안녕하세요! ",
            f"'{message}'에 대한 답변을 생성중입니다. ",
            "현재는 테스트 단계로, ",
            "청크 단위로 응답이 생성되는 것을 시뮬레이션 하고 있습니다. ",
            "이런 방식으로 긴 응답도 점진적으로 확인할 수 있습니다.",
        ]

        for i, chunk in enumerate(chunks):
            current_state = json.loads(redis_client.get(f"task:{task_id}"))

            current_state["response"] += chunk
            current_state["chunks_received"] = i + 1
            current_state["timestamp"] = time.time()

            save_task_to_store(task_id, current_state)

            time.sleep(1)

        # 완료 상태 저장
        final_state = {
            "task_id": task_id,
            "status": "SUCCESS",
            "request": message,
            "response": current_state["response"],
            "chunks_received": current_state["chunks_received"],
            "timestamp": time.time(),
        }
        save_task_to_store(task_id, final_state)

        return final_state

    except Exception as e:
        error_state = {"task_id": task_id, "status": "ERROR", "error": str(e), "timestamp": time.time()}
        save_task_to_store(task_id, error_state)
        return error_state
