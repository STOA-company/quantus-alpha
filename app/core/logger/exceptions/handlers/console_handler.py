"""
콘솔에 예외 정보를 출력하는 핸들러
"""

import sys
from typing import Tuple, Optional

from .base import BaseHandler


class ConsoleHandler(BaseHandler):
    """
    콘솔(터미널)에 예외 정보를 출력하는 핸들러
    """

    # 터미널 색상 코드
    COLORS = {
        "RESET": "\033[0m",
        "RED": "\033[31m",
        "GREEN": "\033[32m",
        "YELLOW": "\033[33m",
        "BLUE": "\033[34m",
        "MAGENTA": "\033[35m",
        "CYAN": "\033[36m",
        "WHITE": "\033[37m",
        "BOLD": "\033[1m",
    }

    def __init__(self, output=sys.stderr, use_colors: Optional[bool] = None, **kwargs):
        """
        콘솔 핸들러 초기화

        Args:
            output: 출력 스트림 (기본값: sys.stderr)
            use_colors: 색상 사용 여부 (기본값: 자동 감지)
            **kwargs: 추가 설정
        """
        super().__init__(**kwargs)
        self.output = output

        # 색상 사용 여부 결정
        if use_colors is None:
            self.use_colors = hasattr(self.output, "isatty") and self.output.isatty()
        else:
            self.use_colors = use_colors

        # Windows에서 색상 지원 활성화
        if self.use_colors and sys.platform.startswith("win"):
            try:
                import colorama

                colorama.init()
            except ImportError:
                self.use_colors = False

    def emit(self, exc_info: Tuple, **context) -> None:
        """
        예외 정보를 콘솔에 출력

        Args:
            exc_info: 예외 정보 튜플 (exc_type, exc_value, exc_traceback)
            **context: 추가 컨텍스트 정보
        """
        if not self.should_notify(context.get("level", self.level)):
            return

        # 예외 정보 포맷팅
        exception_data = self.format_exception(exc_info, **context)

        # 출력 색상 설정
        if self.use_colors:
            bold = self.COLORS["BOLD"]
            red = self.COLORS["RED"]
            yellow = self.COLORS["YELLOW"]
            cyan = self.COLORS["CYAN"]
            reset = self.COLORS["RESET"]
        else:
            bold = red = yellow = cyan = reset = ""

        # 예외 정보 출력
        print(f"\n{bold}{red}===== EXCEPTION ====={reset}", file=self.output)
        print(f"{bold}App:{reset} {exception_data['app_name']} ({exception_data['environment']})", file=self.output)
        print(f"{bold}Type:{reset} {red}{exception_data['type']}{reset}", file=self.output)
        print(f"{bold}Message:{reset} {exception_data['message']}", file=self.output)
        print(f"{bold}Hostname:{reset} {exception_data.get('hostname', 'unknown')}", file=self.output)

        # 컨텍스트 정보 출력
        if context := exception_data.get("context"):
            print(f"\n{bold}Context:{reset}", file=self.output)
            for key, value in context.items():
                if isinstance(value, (str, int, float, bool, type(None))):
                    print(f"  {yellow}{key}{reset}: {value}", file=self.output)
                else:
                    try:
                        value_str = str(value)
                        if len(value_str) > 100:
                            value_str = value_str[:100] + "..."
                        print(f"  {yellow}{key}{reset}: {value_str}", file=self.output)
                    except:  # noqa: E722
                        print(f"  {yellow}{key}{reset}: <unprintable>", file=self.output)

        # 트레이스백 출력
        if traceback := exception_data.get("traceback"):
            print(f"\n{bold}Traceback:{reset}", file=self.output)
            if self.use_colors:
                # 줄 단위로 처리하여 중요 부분 강조
                for line in traceback.split("\n"):
                    if 'File "' in line:
                        parts = line.split('File "', 1)
                        file_part = 'File "' + parts[1]
                        print(parts[0] + cyan + file_part + reset, file=self.output)
                    elif "line" in line and ", in " in line:
                        print(cyan + line + reset, file=self.output)
                    else:
                        print(line, file=self.output)
            else:
                print(traceback, file=self.output)

        # 스택 변수 출력 (첫 번째 프레임만)
        if stack := exception_data.get("stack"):
            if stack and "variables" in stack[0]:
                print(f"\n{bold}Local Variables:{reset}", file=self.output)
                for name, value in stack[0]["variables"].items():
                    print(f"  {cyan}{name}{reset} = {value}", file=self.output)

        print(f"{bold}{red}====================={reset}\n", file=self.output)
        self.output.flush()
