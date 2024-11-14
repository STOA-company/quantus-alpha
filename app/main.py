from fastapi import Depends, FastAPI
from requests import Session
from app.core.config import get_database_config, settings
from app.api import routers
from app.database.conn import db

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="Alphafinder API Documentation",
    version="1.0.0",
)
app.include_router(routers.router)

db_config = get_database_config()
db.init_app(app, **db_config.__dict__)


@app.get("/")
def root():
    return {"message": "Welcome to the Financial Data API !!"}


@app.get("/health-check")
async def health_check(session: Session = Depends(db.get_db)):
    try:
        session.execute("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": str(e)}
