"""
예외 알림 모듈의 핵심 클래스
"""

import functools
import sys
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union, cast

from ..config import Config
from .handlers import ConsoleHandler, DatabaseHandler, FileHandler, SlackHandler
from .handlers.base import BaseHandler

F = TypeVar("F", bound=Callable[..., Any])


def _extract_exc_info(exc: Optional[Exception] = None) -> Tuple:
    """
    예외 정보 추출 유틸리티

    Args:
        exc: 명시적으로 지정한 예외 객체 (None이면 현재 처리 중인 예외)

    Returns:
        (exc_type, exc_value, exc_traceback) 튜플
    """
    if exc is not None:
        return type(exc), exc, exc.__traceback__
    return sys.exc_info()


def _get_function_info(func: Callable) -> Dict[str, Any]:
    """
    함수에 대한 메타 정보를 수집

    Args:
        func: 함수 객체

    Returns:
        함수 메타데이터 정보
    """
    import inspect

    module = inspect.getmodule(func)
    module_name = module.__name__ if module else "unknown"

    # 정규 함수, 메서드, 클래스메서드, staticmethod 처리
    if inspect.ismethod(func):
        func_name = f"{func.__self__.__class__.__name__}.{func.__name__}"
        qualname = f"{module_name}.{func_name}"
    else:
        func_name = getattr(func, "__qualname__", func.__name__)
        qualname = f"{module_name}.{func_name}"

    # 소스 코드 위치 정보
    try:
        filename = inspect.getsourcefile(func) or "unknown"
        lines, start_line = inspect.getsourcelines(func)
    except (OSError, TypeError):
        filename = "unknown"
        start_line = 0

    return {
        "module": module_name,
        "name": func_name,
        "qualname": qualname,
        "filename": filename,
        "lineno": start_line,
    }


class ExceptionNotifier:
    """
    예외 처리 및 알림을 위한 클래스
    """

    def __init__(self, handlers: Optional[List[BaseHandler]] = None, **config_kwargs):
        """
        예외 알리미 초기화

        Args:
            handlers: 사용할 핸들러 목록
            **config_kwargs: 기본 설정을 오버라이드할 설정 값들
        """
        # 설정 구성
        self.config = Config(**config_kwargs)

        # 핸들러가 명시되지 않았다면 기본 설정에서 핸들러 결정
        if handlers is None:
            handlers = self._create_default_handlers()

        self.handlers = handlers

    def _create_default_handlers(self) -> List[BaseHandler]:
        """
        설정에 기반한 기본 핸들러 생성

        Returns:
            기본 핸들러 목록
        """
        handlers = []
        enabled_handlers = self.config.get("exception_handlers", ["file"])

        # 파일 핸들러
        if "file" in enabled_handlers:
            handlers.append(FileHandler())

        # Slack 핸들러
        if "slack" in enabled_handlers and any(url for url in self.config.get("slack_webhook_urls", {}).values()):
            handlers.append(SlackHandler())

        # 데이터베이스 핸들러
        if "database" in enabled_handlers:
            handlers.append(DatabaseHandler())

        # 콘솔 핸들러
        if self.config.get("console_output", True) and "console" in enabled_handlers:
            handlers.append(ConsoleHandler())

        return handlers

    def notify(self, exc_info=None, **context) -> None:
        """
        설정된 모든 핸들러에게 예외 알림 전송

        Args:
            exc_info: 예외 정보 튜플 (None일 경우 sys.exc_info()로 가져옴)
            **context: 추가 컨텍스트 정보
        """
        # 예외 정보가 없으면 현재 처리 중인 예외 정보 가져오기
        if exc_info is None:
            exc_info = sys.exc_info()
            if exc_info == (None, None, None):
                raise ValueError("No exception to notify about")

        # 각 핸들러에게 알림
        for handler in self.handlers:
            try:
                handler.emit(exc_info, **context)
            except Exception as e:
                # 핸들러 자체에서 예외가 발생하면 로깅하지만 다른 핸들러에게는 계속 알림
                print(f"Error in handler {handler.__class__.__name__}: {e}")
                traceback.print_exc()

    def catch(
        self, reraise: bool = True, ignore: Optional[Union[Type[Exception], List[Type[Exception]]]] = None, **context
    ) -> Callable[[F], F]:
        """
        예외를 캐치하고 알림을 보내는 데코레이터

        Args:
            reraise: 처리 후 예외를 다시 발생시킬지 여부
            ignore: 무시할 예외 타입 (또는 타입 목록)
            **context: 추가 컨텍스트 정보

        Returns:
            함수 데코레이터
        """

        def decorator(func: F) -> F:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # 무시할 예외인지 확인
                    if ignore:
                        ignore_types = ignore if isinstance(ignore, list) else [ignore]
                        if any(isinstance(e, exc_type) for exc_type in ignore_types):
                            if reraise:
                                raise
                            return None

                    # 함수 정보 추출
                    func_info = _get_function_info(func)

                    # 컨텍스트에 함수 정보 추가
                    notify_context = {
                        **func_info,
                        **context,
                    }

                    # 인자가 있다면 추가 (간단한 타입만)
                    if kwargs:
                        safe_kwargs = {}
                        for k, v in kwargs.items():
                            # 기본 타입과 간단한 컬렉션만 저장
                            if isinstance(v, (str, int, float, bool, type(None))):
                                safe_kwargs[k] = v
                            elif isinstance(v, (list, tuple, dict)) and len(str(v)) < 100:
                                safe_kwargs[k] = str(v)
                            else:
                                safe_kwargs[k] = f"<{type(v).__name__}>"
                        notify_context["function_kwargs"] = safe_kwargs

                    # 알림 전송
                    self.notify(_extract_exc_info(e), **notify_context)

                    # 필요하면 예외 다시 발생
                    if reraise:
                        raise

                    return None

            return cast(F, wrapper)

        return decorator

    def context_handler(self, reraise: bool = True, **context):
        """
        컨텍스트 관리자로 사용할 수 있는 예외 처리기

        Args:
            reraise: 처리 후 예외를 다시 발생시킬지 여부
            **context: 추가 컨텍스트 정보

        Returns:
            컨텍스트 관리자
        """

        class ContextHandler:
            def __init__(self, notifier, reraise, context):
                self.notifier = notifier
                self.reraise = reraise
                self.context = context

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                if exc_type is not None:
                    self.notifier.notify((exc_type, exc_val, exc_tb), **self.context)
                    return not self.reraise
                return False

        return ContextHandler(self, reraise, context)


# 전역 인스턴스 및 편의 함수

_default_notifier = None


def setup_notifier(**kwargs) -> ExceptionNotifier:
    """
    전역 예외 알리미 설정

    Args:
        **kwargs: 설정 값

    Returns:
        설정된 ExceptionNotifier 인스턴스
    """
    global _default_notifier
    _default_notifier = ExceptionNotifier(**kwargs)
    return _default_notifier


def get_notifier() -> ExceptionNotifier:
    """
    전역 예외 알리미 가져오기

    Returns:
        ExceptionNotifier 인스턴스
    """
    global _default_notifier
    if _default_notifier is None:
        _default_notifier = ExceptionNotifier()
    return _default_notifier


def notify_exception(exc_info=None, **context) -> None:
    """
    전역 예외 알리미를 통해 예외 알림

    Args:
        exc_info: 예외 정보 튜플 (None일 경우 sys.exc_info()로 가져옴)
        **context: 추가 컨텍스트 정보
    """
    notifier = get_notifier()
    notifier.notify(exc_info, **context)


def catch_exceptions(reraise: bool = True, ignore=None, **context):
    """
    예외를 캐치하고 알림을 보내는 데코레이터

    Args:
        reraise: 처리 후 예외를 다시 발생시킬지 여부
        ignore: 무시할 예외 타입 (또는 타입 목록)
        **context: 추가 컨텍스트 정보

    Returns:
        함수 데코레이터
    """
    notifier = get_notifier()
    return notifier.catch(reraise=reraise, ignore=ignore, **context)
