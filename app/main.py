from fastapi import FastAPI
from app.core.config import settings
from app.api import routers

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="Alphafinder API Documentation",
    version="1.0.0",
)
app.include_router(routers.router)


@app.get("/")
def root():
    return {"message": "Welcome to the Financial Data API !!"}
