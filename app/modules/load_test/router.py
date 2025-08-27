from fastapi import APIRouter, Depends
from app.utils.quantus_auth_utils import get_current_user_redis, get_current_user, get_current_user_async

router = APIRouter()

@router.get("/sync")
async def test_endpoint_1(current_user = Depends(get_current_user)):
    return {"message": "Test endpoint 1", "user": current_user}

@router.get("/async") 
async def test_endpoint_2(current_user = Depends(get_current_user_async)):
    return {"message": "Test endpoint 2", "user": current_user}

@router.get("/redis")
async def test_endpoint_3(current_user = Depends(get_current_user_redis)):
    return {"message": "Test endpoint 3", "user": current_user}