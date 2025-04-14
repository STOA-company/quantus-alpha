import base64

from fastapi import UploadFile


def convert_file_to_base64(file: UploadFile) -> str:
    """
    FastAPI의 UploadFile을 base64 문자열로 변환
    """
    contents = file.file.read()
    base64_encoded = base64.b64encode(contents).decode("utf-8")
    file.file.seek(0)  # 파일 포인터를 처음으로

    return base64_encoded
