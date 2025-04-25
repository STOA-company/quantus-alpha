"""
파일 기반 예외 알림 핸들러
"""

import json
import os
import traceback
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from .base import BaseHandler


class FileHandler(BaseHandler):
    """
    파일에 예외 정보를 로깅하는 핸들러
    """

    def __init__(self, file_path: Optional[str] = None, **kwargs):
        """
        파일 핸들러 초기화

        Args:
            file_path: 로그 파일 경로 (None일 경우 설정에서 가져옴)
            **kwargs: 추가 설정
        """
        super().__init__(**kwargs)
        self.file_path = file_path or self.config.get("exception_file_path", "exceptions.log")

        # 파일 디렉토리가 없으면 생성
        directory = os.path.dirname(os.path.abspath(self.file_path))
        os.makedirs(directory, exist_ok=True)

    def emit(self, exc_info: Tuple, **context) -> None:
        """
        예외 정보를 파일에 기록

        Args:
            exc_info: 예외 정보 튜플 (exc_type, exc_value, exc_traceback)
            **context: 추가 컨텍스트 정보
        """
        if not self.should_notify(context.get("level", self.level)):
            return

        # 예외 정보 포맷팅
        exception_data = self.format_exception(exc_info, **context)

        # 현재 시간 추가
        now = datetime.now()
        formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")

        # JSON으로 변환하기 전에 직렬화 불가능한 객체 처리
        serializable_data = self._make_serializable(exception_data)

        try:
            # 파일에 로그 기록
            with open(self.file_path, "a", encoding="utf-8") as f:
                f.write(f"--- Exception at {formatted_time} ---\n")
                f.write(f"Type: {exception_data['type']}\n")
                f.write(f"Message: {exception_data['message']}\n")
                f.write(f"Environment: {exception_data['environment']}\n")
                f.write(f"App: {exception_data['app_name']}\n")
                f.write(f"Hostname: {exception_data.get('hostname', 'unknown')}\n")

                # 트레이스백 작성
                if exception_data.get("traceback"):
                    f.write("\nTraceback:\n")
                    f.write(exception_data["traceback"])

                # 컨텍스트 정보가 있으면 작성
                if exception_data.get("context"):
                    f.write("\nContext:\n")
                    context_json = json.dumps(serializable_data.get("context", {}), indent=2, ensure_ascii=False)
                    f.write(f"{context_json}\n")

                # 스택 정보가 있으면 작성
                if stack := exception_data.get("stack"):
                    f.write("\nStack Variables:\n")
                    for i, frame in enumerate(stack):
                        f.write(f"  Frame {i} ({frame['function']} at {frame['filename']}:{frame['lineno']}):\n")
                        if "context_line" in frame and frame["context_line"]:
                            f.write(f"    Line: {frame['context_line']}\n")
                        if "variables" in frame:
                            f.write("    Variables:\n")
                            for var_name, var_value in frame["variables"].items():
                                f.write(f"      {var_name} = {var_value}\n")

                f.write("\n" + "-" * 70 + "\n\n")
        except Exception as e:
            # 파일 로깅 자체에 실패한 경우, 표준 오류로 출력
            print(f"Error writing to exception log file: {e}")
            traceback.print_exc()

    def _make_serializable(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        딕셔너리를 JSON 직렬화 가능한 형태로 변환

        Args:
            data: 원본 데이터 딕셔너리

        Returns:
            직렬화 가능한 딕셔너리
        """
        result = {}

        for key, value in data.items():
            if isinstance(value, dict):
                result[key] = self._make_serializable(value)
            elif isinstance(value, (str, int, float, bool, type(None))):
                result[key] = value
            else:
                try:
                    # 리스트나 다른 컬렉션은 항목별로 직렬화 시도
                    if isinstance(value, (list, tuple)):
                        result[key] = [
                            self._make_serializable(item)
                            if isinstance(item, dict)
                            else str(item)
                            if not isinstance(item, (str, int, float, bool, type(None)))
                            else item
                            for item in value
                        ]
                    else:
                        # 기타 객체는 문자열로 변환
                        result[key] = str(value)
                except Exception:
                    result[key] = f"<non-serializable: {type(value).__name__}>"

        return result
