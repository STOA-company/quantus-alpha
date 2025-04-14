"""
S3 로그 업로드 유틸리티

로그 폴더를 S3 버킷에 업로드하기 위한 유틸리티 함수를 제공합니다.
"""

import os
import threading
import time

# S3 모듈 import (경로는 실제 프로젝트 구조에 맞게 조정 필요)
from Aws.logic.s3 import get_files_in_s3_directory, upload_dir_to_bucket

# 로그 전용 S3 설정
DEFAULT_LOG_BUCKET = "quantus-logs"  # 실제 프로젝트에 맞는 버킷 이름으로 변경 필요
DEFAULT_LOG_PREFIX = "app-logs"


class S3LogUploader:
    """
    로그 폴더를 S3에 업로드하는 유틸리티 클래스
    """

    def __init__(
        self,
        bucket_name=DEFAULT_LOG_BUCKET,
        prefix=DEFAULT_LOG_PREFIX,
        use_async=True,  # 비동기 업로드 여부
        delete_after_upload=False,  # 업로드 후 로컬 폴더 삭제 여부
    ):
        """
        S3 로그 업로더 초기화

        Args:
            bucket_name: S3 버킷 이름
            prefix: S3 객체 접두사 (폴더 경로)
            use_async: 비동기 업로드 여부 (True면 백그라운드 스레드에서 업로드)
            delete_after_upload: 업로드 후 로컬 폴더 삭제 여부
        """
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.use_async = use_async
        self.delete_after_upload = delete_after_upload
        self.upload_lock = threading.Lock()

    def upload_log_folder(self, local_folder, custom_prefix=None):
        """
        로그 폴더를 S3에 업로드

        Args:
            local_folder: 업로드할 로컬 폴더 경로
            custom_prefix: 사용자 정의 S3 접두사 (기본값 사용하지 않을 경우)

        Returns:
            성공 여부 (비동기 모드에서는 항상 True 반환)
        """
        if not os.path.exists(local_folder) or not os.path.isdir(local_folder):
            print(f"로그 폴더가 존재하지 않습니다: {local_folder}")
            return False

        # 폴더 이름은 날짜 형식(YYYY-MM-DD)을 따를 것으로 예상
        folder_name = os.path.basename(local_folder)

        # S3 업로드 경로 설정
        s3_prefix = custom_prefix or self.prefix
        s3_path = f"{s3_prefix}/{folder_name}"

        # 비동기 모드
        if self.use_async:
            # 백그라운드 스레드에서 업로드
            thread = threading.Thread(target=self._upload_folder_task, args=(local_folder, s3_path), daemon=True)
            thread.start()
            return True

        # 동기 모드
        return self._upload_folder_task(local_folder, s3_path)

    def _upload_folder_task(self, local_folder, s3_path):
        """
        실제 폴더 업로드 작업 수행 (내부 사용)

        Args:
            local_folder: 업로드할 로컬 폴더 경로
            s3_path: S3 업로드 경로

        Returns:
            성공 여부
        """
        try:
            # 중복 업로드 방지
            with self.upload_lock:
                print(f"로그 폴더 업로드 시작: {local_folder} -> s3://{self.bucket_name}/{s3_path}")
                start_time = time.time()

                # 기존 s3.py 모듈의 upload_dir_to_bucket 함수 활용
                upload_dir_to_bucket(bucket=self.bucket_name, local_dir=local_folder, upload_name=s3_path)

                elapsed = time.time() - start_time
                print(f"로그 폴더 업로드 완료: {local_folder} (소요 시간: {elapsed:.2f}초)")

                # 업로드 후 로컬 폴더 삭제 (설정된 경우)
                if self.delete_after_upload:
                    import shutil

                    shutil.rmtree(local_folder)
                    print(f"로컬 로그 폴더 삭제 완료: {local_folder}")

                return True
        except Exception as e:
            print(f"로그 폴더 업로드 중 오류 발생: {e}")
            return False

    def list_uploaded_log_folders(self):
        """
        S3에 업로드된 로그 폴더 목록 조회

        Returns:
            S3에 업로드된 로그 폴더 목록
        """
        try:
            # 기존 s3.py 모듈의 get_files_in_s3_directory 함수 활용
            files = get_files_in_s3_directory(obj_dir=self.prefix, bucket_name=self.bucket_name)

            # 폴더 목록으로 변환 (파일 경로에서 폴더 부분만 추출)
            folders = set()
            for file_path in files:
                parts = file_path.split("/")
                if len(parts) > 0:
                    folders.add(parts[0])

            return sorted(list(folders))
        except Exception as e:
            print(f"S3 로그 폴더 목록 조회 중 오류 발생: {e}")
            return []


# 싱글톤 인스턴스 생성
_default_uploader = None


def get_s3_log_uploader(
    bucket_name=DEFAULT_LOG_BUCKET, prefix=DEFAULT_LOG_PREFIX, use_async=True, delete_after_upload=False
):
    """
    기본 S3 로그 업로더 인스턴스 반환

    Args:
        bucket_name: S3 버킷 이름
        prefix: S3 객체 접두사 (폴더 경로)
        use_async: 비동기 업로드 여부
        delete_after_upload: 업로드 후 로컬 폴더 삭제 여부

    Returns:
        S3LogUploader 인스턴스
    """
    global _default_uploader
    if _default_uploader is None:
        _default_uploader = S3LogUploader(
            bucket_name=bucket_name, prefix=prefix, use_async=use_async, delete_after_upload=delete_after_upload
        )
    return _default_uploader


def upload_log_folder(
    local_folder, bucket_name=DEFAULT_LOG_BUCKET, prefix=DEFAULT_LOG_PREFIX, use_async=True, delete_after_upload=False
):
    """
    로그 폴더를 S3에 편리하게 업로드하는 유틸리티 함수

    Args:
        local_folder: 업로드할 로컬 폴더 경로
        bucket_name: S3 버킷 이름
        prefix: S3 객체 접두사 (폴더 경로)
        use_async: 비동기 업로드 여부
        delete_after_upload: 업로드 후 로컬 폴더 삭제 여부

    Returns:
        성공 여부 (비동기 모드에서는 항상 True 반환)
    """
    uploader = get_s3_log_uploader(
        bucket_name=bucket_name, prefix=prefix, use_async=use_async, delete_after_upload=delete_after_upload
    )
    return uploader.upload_log_folder(local_folder)
