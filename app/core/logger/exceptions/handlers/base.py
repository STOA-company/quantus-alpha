"""
예외 핸들러 기본 클래스
"""

import logging
import socket
from typing import Any, Dict, Tuple

from ...config import get_config
from ..formatter import ExceptionFormatter


class BaseHandler:
    """
    모든 예외 알림 핸들러의 기본 클래스
    """

    def __init__(self, **kwargs):
        """
        핸들러 초기화

        Args:
            **kwargs: 설정 (기본 설정을 오버라이드)
        """
        self.config = get_config()
        self.handler_config = {}
        self.handler_config.update(kwargs)

        # 레벨 설정
        self.level = self.handler_config.get("level", self.config.get("exception_level"))
        if isinstance(self.level, str):
            self.level = getattr(logging, self.level.upper())

        # 포매터 설정
        enable_traceback = self.handler_config.get("enable_traceback", self.config.get("enable_traceback"))
        max_traceback_depth = self.handler_config.get("max_traceback_depth", self.config.get("max_traceback_depth"))
        capture_variables = self.handler_config.get("capture_variables", self.config.get("capture_variables"))

        self.formatter = ExceptionFormatter(
            enable_traceback=enable_traceback,
            max_traceback_depth=max_traceback_depth,
            capture_variables=capture_variables,
        )

    def format_exception(self, exc_info: Tuple, **context) -> Dict[str, Any]:
        """
        예외 정보를 포맷팅

        Args:
            exc_info: 예외 정보 튜플 (exc_type, exc_value, exc_traceback)
            **context: 추가 컨텍스트 정보

        Returns:
            포맷팅된 예외 정보 딕셔너리
        """
        # 예외 정보 기본 포맷팅
        exception_data = self.formatter.format_exception(exc_info, **context)

        # 호스트명 추가
        try:
            exception_data["hostname"] = socket.gethostname()
        except:  # noqa: E722
            exception_data["hostname"] = "unknown"

        # 환경 정보 추가
        exception_data["environment"] = self.config.get("environment")
        exception_data["app_name"] = self.config.get("app_name")

        # 전역 컨텍스트 병합
        global_context = self.config.get("global_context", {})
        if global_context:
            if "context" not in exception_data:
                exception_data["context"] = {}
            exception_data["context"].update(global_context)

        return exception_data

    def should_notify(self, level: int) -> bool:
        """
        주어진 레벨에 대해 알림을 보내야 하는지 결정

        Args:
            level: 로그 레벨

        Returns:
            알림 여부
        """
        # 개발 환경이고 개발 환경에서 알림을 비활성화했다면 알림 안보냄
        if not self.config.get("notify_in_development", False) and self.config.get("environment") == "development":
            return False

        return level >= self.level

    def emit(self, exc_info: Tuple, **context) -> None:
        """
        예외 알림 발행 - 하위 클래스에서 구현해야 합니다.

        Args:
            exc_info: 예외 정보 튜플 (exc_type, exc_value, exc_traceback)
            **context: 추가 컨텍스트 정보
        """
        raise NotImplementedError("Subclasses must implement emit method")
