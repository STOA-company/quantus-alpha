"""
유틸리티 함수 모듈
"""

import json
import traceback
from datetime import datetime
from typing import Any, Dict, Optional, Union, List, Type


def jsonable_encoder(obj: Any) -> Any:
    """
    JSON으로 직렬화할 수 있는 형태로 객체 변환

    Args:
        obj: 변환할 객체

    Returns:
        JSON 직렬화 가능한 객체
    """
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: jsonable_encoder(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, set)):
        return [jsonable_encoder(v) for v in obj]
    elif hasattr(obj, "__dict__"):
        return jsonable_encoder(obj.__dict__)
    else:
        return str(obj)


def format_exception_for_slack(
    exc_type: Type[Exception], exc_value: Exception, exc_traceback: Optional[traceback.TracebackType] = None, **context
) -> str:
    """
    슬랙 메시지 포맷용 예외 정보 문자열 생성

    Args:
        exc_type: 예외 타입
        exc_value: 예외 값
        exc_traceback: 트레이스백 객체
        **context: 추가 컨텍스트 정보

    Returns:
        포맷된 예외 메시지 문자열
    """
    env = context.get("environment", "unknown")
    app = context.get("app_name", "App")

    # 기본 메시지
    message = f"⚠️ *Exception* in `{app}` ({env})\n\n"

    # 예외 타입/메시지
    message += f"*Type:* `{exc_type.__name__}`\n"
    message += f"*Message:* {str(exc_value)}\n"

    # 컨텍스트 정보
    if context:
        message += "\n*Context:*\n"
        for key, value in context.items():
            if key not in ("environment", "app_name") and not key.startswith("_"):
                message += f"• {key}: `{value}`\n"

    # 트레이스백
    if exc_traceback:
        tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        message += f"\n*Traceback:*\n```{tb_str}```"

    return message


def prettify_json(data: Union[Dict, List, Any]) -> str:
    """
    데이터를 읽기 쉬운 JSON 문자열로 변환

    Args:
        data: JSON으로 변환할 데이터

    Returns:
        포맷된 JSON 문자열
    """
    return json.dumps(data, indent=2, ensure_ascii=False, default=jsonable_encoder)
