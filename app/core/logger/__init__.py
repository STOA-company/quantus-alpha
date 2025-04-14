"""
Quant-Notify - 통합 로깅 및 예외 처리 라이브러리

이 라이브러리는 애플리케이션의 로깅과 예외 처리를 위한 통합 솔루션을 제공합니다.
로거와 예외 처리 모듈을 분리된 컴포넌트로 유지하면서도 일관된 설정과 사용 패턴을 지원합니다.
"""

__version__ = "0.1.0"

# 설정 관련 임포트
from .config import configure, get_config

# 예외 관련 임포트
from .exceptions import ExceptionNotifier, catch_exceptions, get_notifier, notify_exception, setup_notifier

# 핸들러 클래스 임포트
from .exceptions.handlers import BaseHandler, ConsoleHandler, DatabaseHandler, FileHandler, SlackHandler

# 로거 관련 임포트
from .logger import Logger, get_logger, setup_logger

__all__ = [
    # 설정
    "configure",
    "get_config",
    # 로거
    "Logger",
    "setup_logger",
    "get_logger",
    # 예외 처리
    "ExceptionNotifier",
    "setup_notifier",
    "get_notifier",
    "notify_exception",
    "catch_exceptions",
    # 핸들러
    "BaseHandler",
    "FileHandler",
    "SlackHandler",
    "DatabaseHandler",
    "ConsoleHandler",
]
