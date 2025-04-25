"""
예외 정보 포맷팅 모듈
"""

import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


class ExceptionFormatter:
    """
    예외 정보를 구조화된 형식으로 변환하는 클래스
    """

    def __init__(self, enable_traceback: bool = True, max_traceback_depth: int = 10, capture_variables: bool = True):
        """
        포매터 초기화

        Args:
            enable_traceback: 트레이스백 포함 여부
            max_traceback_depth: 트레이스백 최대 깊이
            capture_variables: 로컬 변수 캡처 여부
        """
        self.enable_traceback = enable_traceback
        self.max_traceback_depth = max_traceback_depth
        self.capture_variables = capture_variables

    def format_exception(self, exc_info: Tuple, **context) -> Dict[str, Any]:
        """
        예외 정보를 포맷팅

        Args:
            exc_info: 예외 정보 튜플 (exc_type, exc_value, exc_traceback)
            **context: 추가 컨텍스트 정보

        Returns:
            포맷팅된 예외 정보
        """
        exc_type, exc_value, exc_traceback = exc_info

        # 기본 예외 정보
        result = {
            "type": exc_type.__name__,
            "module": exc_type.__module__,
            "message": str(exc_value),
            "timestamp": datetime.utcnow().isoformat(),
            "context": context,
        }

        # 트레이스백 정보 추가
        if self.enable_traceback:
            result["traceback"] = traceback.format_exc()

            # 트레이스백 스택 정보 추가
            if self.capture_variables:
                result["stack"] = self._get_traceback_frames(exc_traceback)

        return result

    def _get_traceback_frames(self, tb) -> List[Dict[str, Any]]:
        """
        트레이스백 프레임 정보 수집

        Args:
            tb: 트레이스백 객체

        Returns:
            프레임 정보 리스트
        """
        frames = []
        depth = 0

        # 트레이스백 프레임 순회
        while tb and depth < self.max_traceback_depth:
            frame = tb.tb_frame

            # 프레임 정보 추출
            frame_info = {
                "filename": frame.f_code.co_filename,
                "lineno": tb.tb_lineno,
                "function": frame.f_code.co_name,
                "context_line": self._get_context_line(frame.f_code.co_filename, tb.tb_lineno),
            }

            # 로컬 변수 추출
            if self.capture_variables:
                variables = {}
                for key, value in frame.f_locals.items():
                    try:
                        # 기본 타입과 간단한 객체만 포함
                        if isinstance(value, (str, int, float, bool, type(None))):
                            variables[key] = value
                        elif isinstance(value, (list, tuple)) and len(value) < 10:
                            variables[key] = f"{type(value).__name__}({len(value)}): {str(value)[:100]}"
                        elif isinstance(value, dict) and len(value) < 10:
                            variables[key] = f"dict({len(value)}): {str(value)[:100]}"
                        else:
                            variables[key] = f"<{type(value).__name__}>"
                    except:  # noqa: E722
                        variables[key] = "<unprintable>"

                frame_info["variables"] = variables

            frames.append(frame_info)
            tb = tb.tb_next
            depth += 1

        return frames

    def _get_context_line(self, filename: str, lineno: int) -> Optional[str]:
        """
        소스 코드에서 특정 줄 가져오기

        Args:
            filename: 파일 경로
            lineno: 줄 번호

        Returns:
            소스 코드 줄 문자열 또는 None
        """
        try:
            lines = open(filename, "r").readlines()
            return lines[lineno - 1].strip() if 0 <= lineno - 1 < len(lines) else None
        except:  # noqa: E722
            return None


def format_error_dict(error: Exception, **kwargs) -> Dict[str, Any]:
    """
    에러를 딕셔너리로 포맷팅하는 유틸리티 함수

    Args:
        error: 예외 객체
        **kwargs: 추가할 데이터

    Returns:
        에러 정보가 포함된 딕셔너리
    """
    if len(error.args) == 0:
        errors = {"error": str(type(error))}
    elif isinstance(error.args[0], dict):
        errors = error.args[0].copy()
    else:
        errors = {"error": str(error.args[0])}

    # 추가 정보 병합
    if kwargs:
        errors.update(kwargs)

    return errors
