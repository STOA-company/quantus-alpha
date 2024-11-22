import logging.config
import os
from typing import Any

from app.core.config import settings
from app.common.directories import logs_dir

LOG_DIR_PATH = logs_dir
LOG_FILE_PATH = os.path.join(LOG_DIR_PATH, "trade_experiment.log")


def configure_logging() -> None:
    """
    로깅 핸들러 설정
    """
    os.makedirs(LOG_DIR_PATH, exist_ok=True)
    logging.config.dictConfig(LOGGING_CONFIG)


LOGGING_CONFIG: dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "[%(asctime)s] %(levelname)s [%(process)d:%(threadName)s] %(message)s",
        },
        "verbose": {
            "format": "[%(asctime)s] %(levelname)s [%(process)d:%(threadName)s] [%(name)s:%(filename)s:%(funcName)s:%(lineno)d] %(message)s",  # noqa: E501
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "formatter": "verbose",
            "filename": LOG_FILE_PATH,
            "when": "midnight",
            "backupCount": 30,
        },
    },
    "loggers": {
        "": {"level": "INFO", "handlers": ["console", "file"], "propagate": False},
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
            },  # 쿼리 생성 로거
        }
    )
