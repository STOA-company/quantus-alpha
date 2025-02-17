import base64
from fastapi import UploadFile


def convert_file_to_base64(file: UploadFile) -> str:
    """
    FastAPI의 UploadFile을 base64 문자열로 변환
    """
    contents = file.read()
    base64_encoded = base64.b64encode(contents).decode("utf-8")
    file.seek(0)

    return base64_encoded
