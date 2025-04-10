"""
로그 포매터 정의
"""

import logging
import sys


class CustomFormatter(logging.Formatter):
    """
    커스텀 로그 포매터

    컬러 지원 및 예외 정보 포맷팅을 향상시킵니다.
    """

    # 터미널 색상 코드
    COLORS = {
        "RESET": "\033[0m",
        "BLACK": "\033[30m",
        "RED": "\033[31m",
        "GREEN": "\033[32m",
        "YELLOW": "\033[33m",
        "BLUE": "\033[34m",
        "MAGENTA": "\033[35m",
        "CYAN": "\033[36m",
        "WHITE": "\033[37m",
        "BOLD": "\033[1m",
    }

    # 로그 레벨별 색상
    LEVEL_COLORS = {
        logging.DEBUG: COLORS["BLUE"],
        logging.INFO: COLORS["GREEN"],
        logging.WARNING: COLORS["YELLOW"],
        logging.ERROR: COLORS["RED"],
        logging.CRITICAL: COLORS["RED"] + COLORS["BOLD"],
    }

    def __init__(self, fmt=None, datefmt=None, style="%", validate=True):
        """
        포매터 초기화

        Args:
            fmt: 포맷 문자열
            datefmt: 날짜 포맷 문자열
            style: 포맷 스타일
            validate: 포맷 검증 여부
        """
        super().__init__(fmt, datefmt, style, validate)

        # 터미널 색상 지원 감지
        self.use_colors = sys.stdout.isatty()

        # Windows에서 색상 지원 활성화
        if self.use_colors and sys.platform.startswith("win"):
            try:
                import colorama

                colorama.init()
            except ImportError:
                self.use_colors = False

    def format(self, record):
        """
        로그 레코드 포맷팅

        Args:
            record: 로그 레코드

        Returns:
            포맷팅된 로그 문자열
        """
        # 기본 포맷팅
        message = super().format(record)

        # 색상 적용 (터미널인 경우)
        if self.use_colors and hasattr(record, "levelno"):
            color = self.LEVEL_COLORS.get(record.levelno, self.COLORS["RESET"])
            reset = self.COLORS["RESET"]

            # 로그 레벨 부분만 색상 적용
            levelname_pos = message.find(record.levelname)
            if levelname_pos != -1:
                levelname_end = levelname_pos + len(record.levelname)
                message = (
                    message[:levelname_pos]
                    + color
                    + message[levelname_pos:levelname_end]
                    + reset
                    + message[levelname_end:]
                )

        return message

    def formatException(self, ei):
        """
        예외 정보 포맷팅

        Args:
            ei: 예외 정보 튜플 (exc_type, exc_value, exc_traceback)

        Returns:
            포맷팅된 예외 문자열
        """
        # 기본 예외 포맷팅
        exception_text = super().formatException(ei)

        # 색상 적용 (터미널인 경우)
        if self.use_colors:
            exception_text = self.COLORS["RED"] + "Exception: " + exception_text + self.COLORS["RESET"]

        return exception_text
