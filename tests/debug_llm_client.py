import asyncio

from app.modules.chat.llm_client import llm_client


async def test():
    print("테스트 시작...")

    # Mock 스트리밍 응답 테스트
    print("Mock 스트리밍 응답 테스트:")
    async for chunk in llm_client._mock_streaming_response("test"):
        print(f"청크: '{chunk}'")

    print("\n실제 스트리밍 응답 테스트:")
    try:
        async for chunk in llm_client.process_query("test", "gpt4mi"):
            print(f"청크: '{chunk}'")
    except Exception as e:
        print(f"오류 발생: {str(e)}")

    print("테스트 완료")


if __name__ == "__main__":
    asyncio.run(test())
