# app/chat/client.py
import time
from typing import Dict, Any
from app.modules.chat.worker import process_chat


class ChatClient:
    def __init__(self):
        pass

    def send_message(self, message: str = "") -> Dict[str, Any]:
        task = process_chat.delay(message)

        return {"task_id": task.id, "status": "PENDING", "timestamp": time.time()}

    def get_response(self, task_id: str, timeout: int = 30, polling_interval: float = 0.5) -> Dict[str, Any]:
        from app.modules.chat.worker import celery_app

        result = celery_app.AsyncResult(task_id)

        start_time = time.time()
        while not result.ready():
            if time.time() - start_time > timeout:
                return {"task_id": task_id, "status": "TIMEOUT", "error": "응답 대기 시간 초과", "timestamp": time.time()}

            time.sleep(polling_interval)

        try:
            response_data = result.get()
            response_data["task_id"] = task_id
            response_data["status"] = "SUCCESS"
            return response_data
        except Exception as e:
            return {"task_id": task_id, "status": "ERROR", "error": str(e), "timestamp": time.time()}
