import asyncio

from app.modules.chat.llm_client import llm_client
from app.modules.chat.service import chat_service


async def test_stream():
    print("스트리밍 테스트 시작...")
    query = "간단한 인사말 부탁드립니다"
    model = "gpt4mi"

    print("1. 실제 LLM API 호출:")
    try:
        print(f"요청: query={query}, model={model}")
        async for chunk in chat_service.process_query(query, model):
            print(f"청크 수신: {chunk}")
    except Exception as e:
        print(f"오류 발생: {str(e)}")

    print("\n2. 모의 응답 직접 호출:")
    try:
        async for chunk in llm_client._mock_streaming_response(query):
            print(f"모의 청크 수신: {chunk}")
    except Exception as e:
        print(f"모의 응답 오류: {str(e)}")

    print("스트리밍 테스트 완료")


if __name__ == "__main__":
    asyncio.run(test_stream())
