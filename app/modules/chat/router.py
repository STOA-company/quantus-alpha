# app/api/chat.py
import json
import os
import time
from typing import Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from redis import Redis

from app.modules.chat.client import ChatClient

router = APIRouter()

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")

redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)

chat_client = ChatClient()


class ChatRequest(BaseModel):
    message: str


class ChatResponse(BaseModel):
    task_id: str
    status: str
    timestamp: float


class ChatResult(BaseModel):
    task_id: str
    status: str
    request: Optional[str] = None
    response: Optional[str] = None
    error: Optional[str] = None
    timestamp: float


def get_task_from_store(task_id: str) -> Optional[Dict]:
    task_data = redis_client.get(f"task:{task_id}")
    if task_data:
        return json.loads(task_data)
    return None


def save_task_to_store(task_id: str, task_data: Dict):
    redis_client.set(f"task:{task_id}", json.dumps(task_data))


@router.post("/send", response_model=ChatResponse)
async def send_message(chat_request: ChatRequest):
    try:
        result = chat_client.send_message(message=chat_request.message)

        task_data = {"status": "PENDING", "timestamp": result["timestamp"]}
        save_task_to_store(result["task_id"], task_data)

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{task_id}", response_model=ChatResult)
async def check_status(task_id: str):
    task_data = get_task_from_store(task_id)
    if not task_data:
        raise HTTPException(status_code=404, detail="태스크를 찾을 수 없습니다")

    if task_data.get("status") in ["SUCCESS", "ERROR", "TIMEOUT"]:
        return task_data

    try:
        response = chat_client.get_response(task_id, timeout=0.1)

        if response["status"] in ["SUCCESS", "ERROR", "TIMEOUT"]:
            save_task_to_store(task_id, response)

        return response
    except Exception as e:
        error_response = {"task_id": task_id, "status": "ERROR", "error": str(e), "timestamp": time.time()}
        save_task_to_store(task_id, error_response)
        return error_response


@router.get("/result/{task_id}", response_model=ChatResult)
async def get_result(task_id: str, timeout: Optional[int] = 30):
    try:
        task_data = get_task_from_store(task_id)
        if not task_data:
            raise HTTPException(status_code=404, detail="태스크를 찾을 수 없습니다")

        if task_data.get("status") in ["SUCCESS", "ERROR", "TIMEOUT"]:
            return task_data

        response = chat_client.get_response(task_id, timeout=timeout)
        save_task_to_store(task_id, response)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
