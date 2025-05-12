from pydantic import BaseModel


class UpdateProfileRequest(BaseModel):
    image_url: str
