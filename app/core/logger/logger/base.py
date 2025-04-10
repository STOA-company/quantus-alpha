"""
로거 기본 클래스 및 유틸리티
"""

import logging
import os
from typing import Optional, Union
import sys
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta

from ..config import get_config
from .formatter import CustomFormatter

# S3 로그 업로더 유틸리티 import (조건부 임포트)
try:
    from ..s3_logger_utils import upload_log_folder

    S3_UPLOADER_AVAILABLE = True
except ImportError:
    S3_UPLOADER_AVAILABLE = False

# SlackHandler 임포트 (조건부 임포트)
try:
    from ..exceptions.handlers.slack_handler import SlackHandler

    SLACK_HANDLER_AVAILABLE = True
except ImportError:
    SLACK_HANDLER_AVAILABLE = False

# 전역 로거 레지스트리
_loggers = {}


class Logger:
    """
    로깅 클래스

    표준 파이썬 로깅 모듈의 래퍼로, 일관된 설정과 사용 패턴을 제공합니다.
    """

    def __init__(self, name: str, level: Optional[Union[int, str]] = None, **kwargs):
        """
        로거 초기화

        Args:
            name: 로거 이름
            level: 로그 레벨 (None일 경우 설정에서 가져옴)
            **kwargs: 추가 설정 (설정 오버라이드)
                log_dir: 로그 디렉토리
                log_format: 로그 포맷
                separate_error_logs: 에러 로그 분리 여부
                use_date_folders: 날짜별 폴더 관리 여부
                date_folder_format: 날짜 폴더 형식
                backup_count: 백업 유지 개수
                upload_to_s3: S3 업로드 여부
                s3_bucket_name: S3 버킷 이름
                s3_prefix: S3 접두사
                s3_upload_async: S3 비동기 업로드 여부
                s3_upload_yesterday: 이전 날짜 폴더만 업로드 여부
                s3_delete_after_upload: 업로드 후 삭제 여부
                use_console_handler: 콘솔 출력 여부 (기본값: True)
                send_error_to_slack: Slack 에러 알림 여부
                slack_webhook_url: Slack 웹훅 URL
                slack_channel: Slack 채널
                slack_username: Slack 사용자 이름
                slack_icon_emoji: Slack 아이콘 이모지
                rotate_logs: 로그 회전 여부
                rotation_interval: 로그 회전 간격
        """
        self.config = get_config()

        # 로거 설정 가져오기
        self.name = name
        self.level = level if level is not None else self.config.get("log_level")
        if isinstance(self.level, str):
            self.level = getattr(logging, self.level.upper())

        # 로그 디렉토리 설정
        self.log_dir = kwargs.get("log_dir", self.config.get("log_dir"))
        os.makedirs(self.log_dir, exist_ok=True)

        # 로그 포맷 설정
        self.log_format = kwargs.get("log_format", self.config.get("log_format"))

        # 에러 로그 분리 여부
        self.separate_error_logs = kwargs.get("separate_error_logs", self.config.get("separate_error_logs"))

        # 날짜별 폴더 관리 설정
        self.use_date_folders = kwargs.get("use_date_folders", self.config.get("use_date_folders", True))
        self.date_folder_format = kwargs.get("date_folder_format", self.config.get("date_folder_format", "%Y-%m-%d"))
        self.backup_count = kwargs.get("backup_count", self.config.get("backup_count", 30))

        # S3 업로드 설정
        self.upload_to_s3 = kwargs.get("upload_to_s3", self.config.get("upload_to_s3", False))
        self.s3_bucket_name = kwargs.get("s3_bucket_name", self.config.get("s3_bucket_name", "quantus-logs"))
        self.s3_prefix = kwargs.get("s3_prefix", self.config.get("s3_prefix", "app-logs"))
        self.s3_upload_async = kwargs.get("s3_upload_async", self.config.get("s3_upload_async", True))
        self.s3_upload_yesterday = kwargs.get("s3_upload_yesterday", self.config.get("s3_upload_yesterday", True))
        self.s3_delete_after_upload = kwargs.get(
            "s3_delete_after_upload", self.config.get("s3_delete_after_upload", False)
        )

        # Slack 알림 설정
        self.send_error_to_slack = kwargs.get("send_error_to_slack", self.config.get("send_error_to_slack", False))
        self.slack_webhook_url = kwargs.get("slack_webhook_url", self.config.get("slack_webhook_url", None))
        self.slack_channel = kwargs.get("slack_channel", self.config.get("default_slack_channel", "default"))
        self.slack_username = kwargs.get("slack_username", self.config.get("slack_username", "Logger Bot"))
        self.slack_icon_emoji = kwargs.get("slack_icon_emoji", self.config.get("slack_icon_emoji", ":warning:"))

        # 이전 날짜 폴더 추적 (S3 업로드에 사용)
        self.current_date_str = datetime.now().strftime(self.date_folder_format)
        self.previous_date_folder = None

        # 날짜 변경 확인 캐싱 설정
        self.last_date_check = datetime.now()
        self.date_check_interval = kwargs.get(
            "date_check_interval", self.config.get("date_check_interval", 60)
        )  # 초 단위, 기본 1분
        self.current_date_folder = None

        # 기존 로그 회전 설정은 유지 (필요시 사용)
        self.rotate_logs = kwargs.get("rotate_logs", self.config.get("rotate_logs", False))
        self.rotation_interval = kwargs.get("rotation_interval", self.config.get("rotation_interval", "daily"))

        # 기본 로거 인스턴스 생성
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self.level)

        # 로그 전파(propagation) 비활성화 - 중복 로깅 방지
        self.logger.propagate = False

        # 콘솔 핸들러 사용 여부 설정
        self.use_console_handler = kwargs.get("use_console_handler", self.config.get("use_console_handler", True))

        # 핸들러가 없으면 초기화
        if not self.logger.handlers:
            self._setup_handlers()

        # 오래된 로그 폴더 정리
        if self.use_date_folders:
            self._cleanup_old_log_folders()

    def _get_current_date_folder(self):
        """현재 날짜에 해당하는 로그 폴더 경로 반환"""
        today = datetime.now().strftime(self.date_folder_format)

        # 날짜가 변경되었으면 이전 날짜 폴더 기록 (S3 업로드용)
        if today != self.current_date_str and self.current_date_str:
            self.previous_date_folder = os.path.join(self.log_dir, self.current_date_str)
            self.current_date_str = today

        date_folder = os.path.join(self.log_dir, today)
        os.makedirs(date_folder, exist_ok=True)

        # 현재 날짜 폴더 캐싱
        self.current_date_folder = date_folder

        return date_folder

    def _upload_previous_folder_to_s3(self):
        """이전 날짜 폴더를 S3에 업로드"""
        if not self.upload_to_s3 or not S3_UPLOADER_AVAILABLE:
            return

        if not self.previous_date_folder or not os.path.exists(self.previous_date_folder):
            return

        try:
            print(f"이전 날짜 폴더 S3 업로드 시작: {self.previous_date_folder}")

            # S3 업로더 유틸리티 사용
            upload_log_folder(
                local_folder=self.previous_date_folder,
                bucket_name=self.s3_bucket_name,
                prefix=self.s3_prefix,
                use_async=self.s3_upload_async,
                delete_after_upload=self.s3_delete_after_upload,
            )

            # 모든 이전 날짜 폴더 업로드가 필요한 경우
            if not self.s3_upload_yesterday:
                # 현재 날짜와 이전 날짜 폴더를 제외한 모든 폴더 업로드
                for folder_name in os.listdir(self.log_dir):
                    folder_path = os.path.join(self.log_dir, folder_name)

                    # 폴더이고, 날짜 형식이며, 현재 날짜 폴더가 아닌 경우
                    if (
                        os.path.isdir(folder_path)
                        and self._is_date_folder(folder_name)
                        and folder_name != self.current_date_str
                        and folder_path != self.previous_date_folder
                    ):
                        print(f"추가 날짜 폴더 S3 업로드: {folder_path}")
                        upload_log_folder(
                            local_folder=folder_path,
                            bucket_name=self.s3_bucket_name,
                            prefix=self.s3_prefix,
                            use_async=self.s3_upload_async,
                            delete_after_upload=self.s3_delete_after_upload,
                        )

        except Exception as e:
            print(f"이전 날짜 폴더 S3 업로드 중 오류 발생: {e}")

    def _cleanup_old_log_folders(self):
        """오래된 로그 폴더 정리"""
        try:
            # 오늘 날짜로부터 backup_count일 이전의 날짜 계산
            cutoff_date = datetime.now() - timedelta(days=self.backup_count)

            # logs 디렉토리 내 모든 날짜 폴더 확인
            for folder_name in os.listdir(self.log_dir):
                folder_path = os.path.join(self.log_dir, folder_name)

                # 디렉토리이고 날짜 형식을 따르는 경우만 처리
                if os.path.isdir(folder_path) and self._is_date_folder(folder_name):
                    try:
                        # 폴더 이름을 날짜 객체로 변환
                        folder_date = datetime.strptime(folder_name, self.date_folder_format)

                        # 기준일보다 오래된 폴더 삭제
                        if folder_date < cutoff_date:
                            # S3 업로드가 활성화된 경우 삭제 전에 업로드
                            if self.upload_to_s3 and S3_UPLOADER_AVAILABLE:
                                upload_log_folder(
                                    local_folder=folder_path,
                                    bucket_name=self.s3_bucket_name,
                                    prefix=self.s3_prefix,
                                    use_async=False,  # 동기 모드로 업로드 (삭제 전 완료 보장)
                                    delete_after_upload=False,  # 업로드 후 수동 삭제
                                )

                            import shutil

                            shutil.rmtree(folder_path)
                            print(f"오래된 로그 폴더 삭제: {folder_path}")
                    except ValueError:
                        # 날짜 변환 실패 시 무시
                        continue
        except Exception as e:
            print(f"로그 폴더 정리 중 오류 발생: {e}")

    def _is_date_folder(self, folder_name):
        """폴더 이름이 날짜 형식인지 확인"""
        try:
            datetime.strptime(folder_name, self.date_folder_format)
            return True
        except ValueError:
            return False

    def _setup_handlers(self):
        """로거 핸들러 설정"""
        # 이미 있는 핸들러 제거
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        # 콘솔 핸들러 추가 (설정에 따라)
        if self.use_console_handler:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(self.level)
            console_handler.setFormatter(CustomFormatter(self.log_format))
            self.logger.addHandler(console_handler)

        # Slack 핸들러 추가 (에러 로그 전용)
        if self.send_error_to_slack and SLACK_HANDLER_AVAILABLE:
            try:
                slack_handler = SlackHandler(
                    webhook_url=self.slack_webhook_url,
                    channel=self.slack_channel,
                    username=self.slack_username,
                    icon_emoji=self.slack_icon_emoji,
                )
                # ERROR 이상 레벨에만 핸들러 적용
                slack_handler.setLevel(logging.ERROR)
                # 간단한 로그 포맷터 적용
                slack_handler.setFormatter(logging.Formatter("%(levelname)s - [%(name)s] %(message)s"))
                self.logger.addHandler(slack_handler)
            except Exception as e:
                # Slack 핸들러 초기화 실패 시 콘솔에 경고 출력
                print(f"Warning: Failed to initialize Slack handler: {e}")

        # 날짜별 폴더에 로그 파일 저장
        if self.use_date_folders:
            date_folder = self._get_current_date_folder()
            log_file = os.path.join(date_folder, f"{self.name}.log")

            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter(self.log_format))

            # 에러 로그 분리 설정
            if self.separate_error_logs:
                # 일반 로그 파일에는 INFO 이하 레벨만 저장
                file_handler.setLevel(self.level)
                file_handler.addFilter(lambda record: record.levelno < logging.ERROR)

                # 에러 로그 파일에는 ERROR 이상 레벨만 저장
                error_log_file = os.path.join(date_folder, f"{self.name}_error.log")
                error_handler = logging.FileHandler(error_log_file)
                error_handler.setLevel(logging.ERROR)
                error_handler.setFormatter(logging.Formatter(self.log_format))
                self.logger.addHandler(error_handler)
            else:
                # 에러 로그를 분리하지 않으면 모든 레벨 저장
                file_handler.setLevel(self.level)

            self.logger.addHandler(file_handler)

        # 기존 방식 (날짜별 폴더 사용 안 함)
        else:
            log_file = os.path.join(self.log_dir, f"{self.name}.log")

            if self.rotate_logs:
                # 일자별 로그 회전 설정
                if self.rotation_interval == "daily":
                    when = "midnight"
                    interval = 1
                    suffix_format = "%Y-%m-%d"
                elif self.rotation_interval == "hourly":
                    when = "H"
                    interval = 1
                    suffix_format = "%Y-%m-%d_%H"
                elif self.rotation_interval == "weekly":
                    when = "W0"  # 월요일에 회전
                    interval = 1
                    suffix_format = "%Y-%m-%d"
                elif self.rotation_interval == "monthly":
                    when = "MIDNIGHT"
                    interval = 30  # 약 30일마다 회전
                    suffix_format = "%Y-%m-%d"
                else:
                    when = "midnight"  # 기본값은 일일 회전
                    interval = 1
                    suffix_format = "%Y-%m-%d"

                file_handler = TimedRotatingFileHandler(
                    log_file, when=when, interval=interval, backupCount=self.backup_count
                )
                file_handler.suffix = f".{suffix_format}"
            else:
                file_handler = logging.FileHandler(log_file)

            file_handler.setFormatter(logging.Formatter(self.log_format))

            # 에러 로그 분리 설정
            if self.separate_error_logs:
                # 일반 로그 파일에는 INFO 이하 레벨만 저장
                file_handler.setLevel(self.level)
                file_handler.addFilter(lambda record: record.levelno < logging.ERROR)

                # 에러 로그 파일에는 ERROR 이상 레벨만 저장
                error_log_file = os.path.join(self.log_dir, f"{self.name}_error.log")

                if self.rotate_logs:
                    error_handler = TimedRotatingFileHandler(
                        error_log_file, when=when, interval=interval, backupCount=self.backup_count
                    )
                    error_handler.suffix = f".{suffix_format}"
                else:
                    error_handler = logging.FileHandler(error_log_file)

                error_handler.setLevel(logging.ERROR)
                error_handler.setFormatter(logging.Formatter(self.log_format))
                self.logger.addHandler(error_handler)
            else:
                # 에러 로그를 분리하지 않으면 모든 레벨 저장
                file_handler.setLevel(self.level)

            self.logger.addHandler(file_handler)

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

        # 기존 파일 핸들러 확인
        for handler in self.logger.handlers[:]:
            # 파일 핸들러만 확인
            if isinstance(handler, logging.FileHandler) and not isinstance(handler, logging.StreamHandler):
                # 현재 핸들러의 파일 경로가 현재 날짜 폴더에 있는지 확인
                if os.path.dirname(handler.baseFilename) != current_date_folder:
                    # 새로운 날짜 폴더로 핸들러 업데이트
                    self.logger.removeHandler(handler)

        # 파일 핸들러가 없으면 새로 설정
        has_file_handler = any(
            isinstance(h, logging.FileHandler) and not isinstance(h, logging.StreamHandler) for h in self.logger.handlers
        )
        if not has_file_handler:
            self._setup_handlers()

    def debug(self, msg: str, *args, **kwargs):
        """디버그 로그"""
        self._check_date_change()
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):
        """정보 로그"""
        self._check_date_change()
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):
        """경고 로그"""
        self._check_date_change()
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        """에러 로그"""
        self._check_date_change()
        self.logger.error(msg, *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs):
        """치명적 에러 로그"""
        self._check_date_change()
        self.logger.critical(msg, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs):
        """예외 로그 (traceback 포함)"""
        self._check_date_change()
        self.logger.exception(msg, *args, **kwargs)

    def log(self, level: Union[int, str], msg: str, *args, **kwargs):
        """특정 레벨 로그"""
        self._check_date_change()
        if isinstance(level, str):
            level = getattr(logging, level.upper())
        self.logger.log(level, msg, *args, **kwargs)

    # 로그 핸들러를 강제로 업데이트하는 메서드 추가 (필요시 수동 호출)
    def force_handler_update(self):
        """로그 핸들러를 강제로 업데이트 (날짜 변경 시 수동 호출용)"""
        self.last_date_check = datetime.min  # 마지막 체크 시간을 오래전으로 설정하여 강제 업데이트
        self._check_date_change()


def setup_logger(name: str, **kwargs) -> Logger:
    """
    로거 설정

    Args:
        name: 로거 이름
        **kwargs: 로거 설정
            use_console_handler: 콘솔 출력 사용 여부 (기본값: True)
            level: 로그 레벨
            log_dir: 로그 디렉토리
            etc: 그 외 Logger 클래스 초기화 매개변수

    Returns:
        설정된 Logger 인스턴스
    """
    global _loggers
    logger = Logger(name, **kwargs)
    _loggers[name] = logger
    return logger


def get_logger(name: str) -> Logger:
    """
    로거 가져오기

    Args:
        name: 로거 이름

    Returns:
        Logger 인스턴스 (없으면 생성)

    Notes:
        기본 설정으로 로거가 생성됩니다. 콘솔 출력 등 설정을 변경하려면 setup_logger를 사용하세요.
    """
    global _loggers
    if name not in _loggers:
        _loggers[name] = Logger(name)
    return _loggers[name]
