from fastapi import FastAPI
from app.core.config import settings
from app.api import routers

app = FastAPI(title=settings.PROJECT_NAME)
app.include_router(routers.router)


@app.get("/")
def root():
    return {"message": "Welcome to the Financial Data API !!"}
