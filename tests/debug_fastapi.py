import asyncio

import uvicorn
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

app = FastAPI()


# 테스트용 비동기 생성기
async def test_generator():
    for i in range(5):
        yield f"chunk {i}\n"
        await asyncio.sleep(1)


@app.get("/test-stream")
async def test_stream():
    return StreamingResponse(test_generator(), media_type="text/event-stream")


# 직접 실행하기 위한 코드
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
