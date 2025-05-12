from fastapi import APIRouter, Depends

from app.models.models_users import AlphafinderUser
from app.modules.common.schemas import BaseResponse
from app.modules.community.schemas import PresignedUrlRequest
from app.modules.community.v2.schemas import PresignedUrlResponse
from app.modules.user.v2.schemas import UpdateProfileRequest
from app.modules.user.v2.service import UserService, get_user_service
from app.utils.quantus_auth_utils import get_current_user

router = APIRouter()


# presigned url 생성
@router.post(
    "/presigned-url",
    summary="presigned url 생성",
    response_model=BaseResponse[PresignedUrlResponse],
    description="""
    presigned url을 생성하는 API입니다.
    프로필 이미지 업로드를 위한 presigned URL을 생성합니다.
""",
)
def generate_presigned_url(
    request: PresignedUrlRequest,
    service: UserService = Depends(get_user_service),
):
    """
    Generate a presigned URL for uploading a profile image.

    Args:
        content_type (str): Content type of the file (e.g., 'image/jpeg', 'image/png')
        file_size (int): Size of the file in bytes
        current_user (AlphafinderUser): Current authenticated user
        service (UserService): User service instance

    Returns:
        dict: Dictionary containing upload URL and image key
    """
    result = service.generate_presigned_url(
        content_type=request.content_type,
        file_size=request.file_size,
    )
    presigned_url_response = PresignedUrlResponse(
        upload_url=result["upload_url"],
        image_key=result["image_key"],
        image_index=0,
    )
    return BaseResponse(status_code=200, message="Presigned URL generated successfully", data=presigned_url_response)


# # 프로필 가져오기
# @router.get("/profile", summary="프로필 가져오기", description="""
#     프로필 정보를 가져오는 API입니다.
#     - 프로필 이미지
#     - 닉네임
#     - 소개
#     - 링크
# """)
# def get_profile(
#     current_user: AlphafinderUser = Depends(get_current_user),
#     service: UserService = Depends(get_user_service),
# ):
#     return service.get_profile(user_id=current_user["uid"])

# # 프로필 수정
# @router.put("/profile", summary="프로필 수정", description="""
#     프로필 정보를 수정하는 API입니다.
#     - 닉네임
#     - 소개
#     - 링크
# """)
# def update_profile(
#     nickname: str = None,
#     introduction: str = None,
#     link: str = None,
#     current_user: AlphafinderUser = Depends(get_current_user),
#     service: UserService = Depends(get_user_service),
# ):
#     is_success = service.update_profile(user_id=current_user["uid"], nickname=nickname, introduction=introduction, link=link)
#     if not is_success:
#         raise HTTPException(status_code=400, detail="Failed to update profile")
#     return BaseResponse(status_code=200, message="Profile updated successfully")


# 프로필 이미지 수정
@router.patch(
    "/profile-image",
    summary="프로필 이미지 업데이트",
    description="""
    프로필 이미지를 업데이트하는 API입니다.

    # 기본 이미지 키 값
    개구리 : default_image/Frog.png
    독수리 : default_image/Vulture.png
    알파카 : default_image/Alpaca.png
    고양이 : default_image/Cat.png
    돼지 : default_image/Pig.png
    도마뱀 : default_image/Lizard.png
    하마 : default_image/Hippo.png
    수달 : default_image/Otter.png
""",
)
def update_profile_image(
    image_url: UpdateProfileRequest,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: UserService = Depends(get_user_service),
):
    service.update_profile_image(user_id=current_user["uid"], image_url=image_url.image_url)

    return BaseResponse(status_code=200, message="Profile image updated successfully")


# @router.get("/duplicate", summary="닉네임 중복 확인")
# def check_nickname_duplicate(nickname: str, service: UserService = Depends(get_user_service)):
#     is_available = service.check_nickname_available(nickname)
#     return {"is_available": is_available}
