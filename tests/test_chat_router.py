import pytest
from httpx import AsyncClient
import asyncio

from app.main import app


@pytest.mark.asyncio
async def test_chat_flow():
    """채팅의 기본 플로우를 테스트합니다:
    1. 메시지 전송 후 task_id 받기
    2. task_id로 결과 조회하기
    """
    async with AsyncClient(app=app, base_url="http://test") as client:
        # 1. 메시지 전송하고 task_id 받기
        req = {"message": "테스트 메시지입니다."}
        response = await client.post("/chat/send", json=req)
        assert response.status_code == 200
        data = response.json()
        assert "task_id" in data

        # 2. task_id로 결과 조회하기 (최대 3번 시도)
        task_id = data["task_id"]
        for _ in range(3):
            result_response = await client.get(f"/chat/result/{task_id}")
            if result_response.status_code == 200:
                result_data = result_response.json()
                print(f"Response data: {result_data}")  # 디버깅용 출력
                if result_data["status"] == "SUCCESS":
                    assert "response" in result_data
                    assert len(result_data["response"]) > 0
                    break
            await asyncio.sleep(1)
