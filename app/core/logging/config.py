import logging.config
from typing import Any

from app.core.config import settings


def configure_logging() -> None:
    """
    로깅 설정
    """
    logging.config.dictConfig(LOGGING_CONFIG)


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
