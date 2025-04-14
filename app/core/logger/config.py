"""
설정 관리 모듈

로거와 예외 처리 모듈의 공통 설정을 관리합니다.
"""

import logging
import os
import threading
from typing import Dict, Any
from datetime import datetime

# 로그 레벨 정의 (표준 로깅 모듈과 호환)
DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL

# 기본 설정
_DEFAULT_CONFIG = {
    # 공통 설정
    "environment": os.environ.get("ENV", "dev"),
    "app_name": "ERROR_LOGGER",
    # 로거 설정
    "log_level": INFO,
    "log_dir": "logs",
    "log_format": "%(asctime)s - %(levelname)s - [%(name)s] %(message)s",
    "separate_error_logs": True,
    "use_console_handler": True,  # 콘솔 출력 사용 여부
    # 날짜별 폴더 관리 설정
    "use_date_folders": True,  # 날짜별 폴더 사용 여부
    "date_folder_format": "%Y-%m-%d",  # 날짜 폴더 형식 (예: 2025-04-09)
    "backup_count": 30,  # 보관할 날짜 폴더 수
    "date_check_interval": 60,  # 날짜 변경 확인 간격 (초 단위, 기본 1분)
    # S3 업로드 설정
    "upload_to_s3": False,  # S3 업로드 활성화 여부
    "s3_bucket_name": "quantus-logs",  # S3 버킷 이름
    "s3_prefix": "app-logs",  # S3 접두사 (폴더)
    "s3_upload_async": True,  # 비동기 업로드 여부
    "s3_upload_yesterday": True,  # 어제 날짜 폴더만 업로드 (False면 모든 이전 폴더 업로드)
    "s3_delete_after_upload": False,  # 업로드 후 로컬 폴더 삭제 여부
    # Slack 알림 설정
    "send_error_to_slack": False,  # 에러 로그를 Slack으로 전송할지 여부
    "slack_webhook_url": None,  # Slack 웹훅 URL (직접 지정 시)
    "slack_username": "Logger Bot",  # Slack 봇 이름
    "slack_icon_emoji": ":warning:",  # Slack 봇 아이콘
    # 기존 로그 회전 설정 (날짜별 폴더를 사용하지 않을 때)
    "rotate_logs": False,  # 로그 파일 회전 활성화 여부
    "rotation_interval": "daily",  # 로그 회전 간격 ('daily', 'hourly', 'weekly', 'monthly')
    # 예외 처리 설정
    "exception_level": ERROR,
    "exception_handlers": ["file"],  # 기본 핸들러 목록
    "slack_webhook_urls": {
        "default": None,
        "trade": None,
        "payment": None,
        "user": None,
        "front_web": None,
        "front_mobile": None,
    },
    "default_slack_channel": "default",
    "db_url": "sqlite:///exceptions.db",
    "exception_file_path": "logs/exceptions.log",
    "enable_traceback": True,
    "max_traceback_depth": 10,
    "capture_variables": True,
    "include_context": True,
    "global_context": {},  # 전역 컨텍스트 정보
    "notify_in_development": False,  # 개발 환경에서 알림 전송 여부
    "console_output": True,  # 콘솔에 예외 출력 여부
}

# 싱글톤 설정 객체를 위한 스레드 로컬 스토리지
_thread_local = threading.local()


class Config:
    """설정 관리 클래스"""

    def __init__(self, **kwargs):
        """
        설정 초기화

        Args:
            **kwargs: 오버라이드할 설정 값들
        """
        self._config = _DEFAULT_CONFIG.copy()
        self.update(**kwargs)

    def update(self, **kwargs):
        """
        설정 업데이트

        Args:
            **kwargs: 업데이트할 설정 값들
        """
        # 특별 처리가 필요한 설정
        if "slack_webhook_urls" in kwargs and isinstance(kwargs["slack_webhook_urls"], dict):
            # 기존 웹훅 URL 딕셔너리와 병합
            current_urls = self._config.get("slack_webhook_urls", {})
            current_urls.update(kwargs.pop("slack_webhook_urls"))
            self._config["slack_webhook_urls"] = current_urls

        # 나머지 설정 업데이트
        self._config.update(kwargs)

    def get(self, key: str, default: Any = None) -> Any:
        """
        설정 값 조회

        Args:
            key: 설정 키
            default: 설정이 없을 경우 기본값

        Returns:
            설정 값
        """
        return self._config.get(key, default)

    def __getitem__(self, key: str) -> Any:
        """
        설정 값 조회 (딕셔너리 스타일)

        Args:
            key: 설정 키

        Returns:
            설정 값

        Raises:
            KeyError: 설정 키가 없을 경우
        """
        return self._config[key]

    def __setitem__(self, key: str, value: Any):
        """
        설정 값 업데이트 (딕셔너리 스타일)

        Args:
            key: 설정 키
            value: 설정 값
        """
        self._config[key] = value

    def as_dict(self) -> Dict[str, Any]:
        """
        모든 설정을 딕셔너리로 반환

        Returns:
            설정 딕셔너리
        """
        return self._config.copy()

    def _check_date_change(self):
        """날짜가 변경되었는지 확인하고 핸들러 업데이트"""
        if not self.use_date_folders:
            return

        # 마지막 체크 이후 설정된 간격보다 적게 지났으면 건너뜀
        now = datetime.now()
        if (now - self.last_date_check).total_seconds() < self.date_check_interval:
            return

        # 간격이 지나면 날짜 변경 검사 수행
        self.last_date_check = now

        current_date_folder = self._get_current_date_folder()

        # S3 업로드 로직 제거 (외부 스크립트로 이동)
        # 날짜 변경이 감지되면 이전 날짜 폴더 정보만 기록

        # 기존 파일 핸들러 확인 및 업데이트 (이 부분은 유지)
        for handler in self.logger.handlers[:]:
            if isinstance(handler, logging.FileHandler) and not isinstance(handler, logging.StreamHandler):
                if os.path.dirname(handler.baseFilename) != current_date_folder:
                    self.logger.removeHandler(handler)

        # 파일 핸들러가 없으면 새로 설정
        has_file_handler = any(
            isinstance(h, logging.FileHandler) and not isinstance(h, logging.StreamHandler) for h in self.logger.handlers
        )
        if not has_file_handler:
            self._setup_handlers()


def configure(**kwargs) -> Config:
    """
    전역 설정 구성

    Args:
        **kwargs: 설정할 값들

    Returns:
        업데이트된 설정 객체
    """
    config = getattr(_thread_local, "config", None)
    if config is None:
        config = Config(**kwargs)
        _thread_local.config = config
    else:
        config.update(**kwargs)
    return config


def get_config() -> Config:
    """
    현재 설정 객체 반환

    Returns:
        현재 설정 객체
    """
    config = getattr(_thread_local, "config", None)
    if config is None:
        config = Config()
        _thread_local.config = config
    return config
