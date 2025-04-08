import logging.config
from typing import Any
import os
import logging
import logging.handlers
import json
from datetime import datetime

from app.core.config import settings


def configure_logging() -> None:
    """
    로깅 설정
    """
    # 로그 디렉토리 확인 및 생성
    log_dir = os.path.join(os.getcwd(), "log")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # 콘솔 핸들러
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console.setFormatter(console_formatter)
    root_logger.addHandler(console)

    # 파일 핸들러 (일반 로그)
    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "app.log"),
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
    )
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # API 모니터링 로거 설정
    api_logger = logging.getLogger("api_monitoring")
    api_logger.setLevel(logging.INFO)
    api_logger.propagate = False  # 루트 로거로 전파 방지

    # API 모니터링용 파일 핸들러
    api_file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "api_monitoring.log"),
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
    )
    api_file_handler.setLevel(logging.INFO)

    # JSON 형식 로깅을 위한 커스텀 포매터
    class JsonFormatter(logging.Formatter):
        def format(self, record):
            log_record = {
                "timestamp": datetime.now().isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }

            # 이미 JSON 문자열인 경우 파싱하여 병합
            if (
                record.message.startswith("API:")
                or record.message.startswith("SLOW API:")
                or record.message.startswith("API ERROR:")
            ):
                try:
                    # "API: {"type":"api_request",...}" 형식에서 JSON 추출
                    prefix, json_str = record.message.split(":", 1)
                    json_data = json.loads(json_str.strip())
                    log_record.update(json_data)
                    log_record["event"] = prefix.strip()
                except (ValueError, json.JSONDecodeError):
                    pass

            if hasattr(record, "exc_info") and record.exc_info:
                log_record["exception"] = self.formatException(record.exc_info)

            return json.dumps(log_record)

    api_formatter = JsonFormatter()
    api_file_handler.setFormatter(api_formatter)
    api_logger.addHandler(api_file_handler)

    # 에러 전용 로그 파일 (심각한 문제만)
    error_file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "error.log"),
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
    )
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(file_formatter)
    root_logger.addHandler(error_file_handler)

    # API 에러 로그 (별도 파일)
    api_error_file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "api_error.log"),
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
    )
    api_error_file_handler.setLevel(logging.ERROR)
    api_error_file_handler.setFormatter(api_formatter)
    api_logger.addHandler(api_error_file_handler)

    logging.info("로깅 시스템이 초기화되었습니다.")


LOGGING_CONFIG: dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "[%(asctime)s] %(levelname)s [%(name)s:%(funcName)s:%(lineno)d] %(message)s",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "": {  # root logger
            "level": "INFO",
            "handlers": ["console"],
            "propagate": False,
        },
    },
}

if settings.DEBUG:
    # debugging sqlalchemy
    LOGGING_CONFIG["loggers"].update(
        {
            "sqlalchemy.engine.Engine": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False,
            },
        }
    )


def get_logger(name: str) -> logging.Logger:
    """
    로거 인스턴스를 반환하는 유틸리티 함수
    """
    return logging.getLogger(name)
