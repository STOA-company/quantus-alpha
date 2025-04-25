"""
Slack 웹훅을 통한 로깅 및 예외 알림 핸들러
"""

import json
import logging
import traceback
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, Union
from urllib.error import URLError
from urllib.request import Request, urlopen

from .base import BaseHandler


class SlackHandler(BaseHandler, logging.Handler):
    """
    Slack 웹훅을 통해 로그 메시지 및 예외 알림을 보내는 핸들러

    logging.Handler를 상속하여 일반 로깅도 지원합니다.
    """

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        channel: Optional[str] = None,
        username: str = "Notify",
        icon_emoji: str = ":warning:",
        level: int = logging.ERROR,  # 기본적으로 ERROR 레벨 이상만 처리
        **kwargs,
    ):
        """
        Slack 핸들러 초기화

        Args:
            webhook_url: Slack 웹훅 URL (None일 경우 설정에서 가져옴)
            channel: 알림을 보낼 채널 키 (webhook_urls 딕셔너리의 키)
            username: Slack에 표시될 봇 이름
            icon_emoji: Slack에 표시될 봇 아이콘 이모지
            level: 로깅 레벨 (기본값: ERROR)
            **kwargs: 추가 설정
        """
        # BaseHandler 초기화
        BaseHandler.__init__(self, **kwargs)
        # 로깅 핸들러 초기화
        logging.Handler.__init__(self, level)

        # 웹훅 URL 설정
        self.webhook_urls = self.config.get("slack_webhook_urls", {})
        self.channel = channel or self.config.get("default_slack_channel")

        # 직접 URL이 주어진 경우 우선 사용
        if webhook_url:
            self.webhook_url = webhook_url
        # 채널명이 주어진 경우 해당 채널의 웹훅 URL 사용
        elif self.channel and self.channel in self.webhook_urls:
            self.webhook_url = self.webhook_urls[self.channel]
        # 기본 채널의 웹훅 URL 사용
        elif "default" in self.webhook_urls:
            self.webhook_url = self.webhook_urls["default"]
        else:
            self.webhook_url = None

        self.username = username
        self.icon_emoji = icon_emoji

    def emit(self, record_or_exc_info: Union[logging.LogRecord, Tuple], **context) -> None:
        """
        로그 레코드 또는 예외 정보를 Slack으로 전송

        Args:
            record_or_exc_info: 로그 레코드 또는 예외 정보 튜플
            **context: 추가 컨텍스트 정보
        """
        # 메시지를 처리할 수 있는지 확인
        if not self.webhook_url:
            print("Warning: Slack webhook URL not configured, skipping notification")
            return

        # 로그 레코드인지 예외 정보인지 확인
        if isinstance(record_or_exc_info, logging.LogRecord):
            # 로그 레코드 처리
            self._emit_log_record(record_or_exc_info)
        else:
            # 예외 정보 처리 (기존 로직)
            self._emit_exception(record_or_exc_info, **context)

    def _emit_log_record(self, record: logging.LogRecord) -> None:
        """
        로그 레코드를 Slack으로 전송

        Args:
            record: 로그 레코드
        """
        try:
            # 레코드 포맷팅
            msg = self.format(record)

            # If this is an exception log, remove the traceback from the message
            # to avoid duplicate tracebacks in Slack messages
            if record.exc_info:
                # Get only the first line of the message (before traceback)
                msg_lines = msg.split("\n")
                msg = msg_lines[0]  # Just keep the message part, without traceback

            # 로그 레벨에 따른 색상 설정
            if record.levelno >= logging.CRITICAL:
                color = "#7b0000"  # 짙은 빨간색
                emoji = "❗"  # 빨간 느낌표
            elif record.levelno >= logging.ERROR:
                color = "#ff0000"  # 빨간색
                emoji = "⚠️"  # 경고
            elif record.levelno >= logging.WARNING:
                color = "#ffcc00"  # 노란색
                emoji = "⚠️"  # 경고
            elif record.levelno >= logging.INFO:
                color = "#36a64f"  # 녹색
                emoji = "ℹ️"  # 정보
            else:
                color = "#439fe0"  # 파란색
                emoji = "🔍"  # 돋보기

            # Slack 메시지 생성
            message = {
                "username": self.username,
                "icon_emoji": self.icon_emoji,
                "text": f"{emoji} *{record.levelname}*: {msg}",
                "attachments": [
                    {
                        "color": color,
                        "fields": [
                            {"title": "Logger", "value": record.name, "short": True},
                            {
                                "title": "Time",
                                "value": datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S"),
                                "short": True,
                            },
                        ],
                    }
                ],
            }

            # 예외 정보가 있으면 트레이스백 추가
            if record.exc_info:
                exc_type, exc_value, exc_tb = record.exc_info
                tb_text = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))

                # 트레이스백이 너무 길면 자름
                if len(tb_text) > 3000:
                    tb_text = tb_text[:3000] + "...\n[Traceback truncated]"

                message["attachments"].append(
                    {"color": color, "title": "Exception", "text": f"```{tb_text}```", "mrkdwn_in": ["text"]}
                )

            # Slack 웹훅 호출
            self._send_to_slack(message)

        except Exception as e:
            print(f"Error sending log to Slack: {e}")
            # 핸들러 내부에서 로깅하면 무한 루프가 발생할 수 있으므로 print 사용

    def _emit_exception(self, exc_info: Tuple, **context) -> None:
        """
        예외 정보를 Slack으로 전송 (기존 emit 메서드)

        Args:
            exc_info: 예외 정보 튜플 (exc_type, exc_value, exc_traceback)
            **context: 추가 컨텍스트 정보
        """
        if not self.should_notify(context.get("level", self.level)):
            return

        if not self.webhook_url:
            print("Warning: Slack webhook URL not configured, skipping notification")
            return

        # context에서 channel 파라미터 추출 (있을 경우)
        if slack_channel := context.pop("slack_channel", None):
            if slack_channel in self.webhook_urls:
                webhook_url = self.webhook_urls[slack_channel]
            else:
                webhook_url = self.webhook_url
        else:
            webhook_url = self.webhook_url

        # 예외 정보 포맷팅
        exception_data = self.format_exception(exc_info, **context)

        # Slack 메시지 생성
        message = self._create_slack_message(exception_data)

        # 웹훅 URL 설정
        self.webhook_url = webhook_url

        # Slack으로 전송
        self._send_to_slack(message)

    def _send_to_slack(self, message: Dict[str, Any]) -> None:
        """
        Slack 웹훅을 통해 메시지 전송

        Args:
            message: Slack 메시지 페이로드
        """
        try:
            headers = {"Content-Type": "application/json"}
            data = json.dumps(message).encode("utf-8")
            request = Request(self.webhook_url, data=data, headers=headers)

            response = urlopen(request, timeout=5)
            if response.getcode() != 200:
                print(f"Slack API error: {response.read().decode('utf-8')}")

        except URLError as e:
            print(f"Error sending Slack notification: {e}")
        except Exception as e:
            print(f"Unexpected error sending Slack notification: {e}")
            traceback.print_exc()

    def _create_slack_message(self, exception_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Slack 메시지 페이로드 생성

        Args:
            exception_data: 포맷팅된 예외 정보

        Returns:
            Slack API 메시지 페이로드
        """
        exc_type = exception_data["type"]
        exc_message = exception_data["message"]
        env = exception_data["environment"]
        app_name = exception_data["app_name"]

        # 기본 슬랙 메시지
        message = {
            "username": self.username,
            "icon_emoji": self.icon_emoji,
            "text": f"⚠️ *Exception* in `{app_name}` ({env})",
            "attachments": [
                {
                    "color": "#ff0000",  # 빨간색 사이드바
                    "title": f"{exc_type}: {exc_message}",
                    "fields": [],
                }
            ],
        }

        # 요청 정보 추출 및 추가
        context = exception_data.get("context", {})

        # 요청 URL 및 메서드 정보 추가
        request_url = context.get("request_url")
        if request_url:
            message["attachments"][0]["fields"].append({"title": "Request URL", "value": request_url, "short": False})

        request_method = context.get("request_method")
        if request_method:
            message["attachments"][0]["fields"].append(
                {"title": "Request Method", "value": request_method, "short": True}
            )

        # 클라이언트 IP 정보 추가
        client_ip = context.get("client_ip")
        if client_ip:
            message["attachments"][0]["fields"].append({"title": "Client IP", "value": client_ip, "short": True})

        # 요청 쿼리 파라미터 추가
        request_query = context.get("request_query")
        if request_query:
            message["attachments"][0]["fields"].append(
                {"title": "Query Parameters", "value": f"```{request_query}```", "short": False}
            )

        # 요청 바디 추가
        request_body = context.get("request_body")
        if request_body:
            # 요청 바디가 너무 길면 잘라냄
            if len(str(request_body)) > 1000:
                body_display = str(request_body)[:1000] + "..."
            else:
                body_display = str(request_body)

            message["attachments"][0]["fields"].append(
                {"title": "Request Body", "value": f"```{body_display}```", "short": False}
            )

        # 기타 컨텍스트 정보 추가 (중요 정보만)
        for key, value in context.items():
            # 이미 추가한 필드는 스킵
            if key in ["request_url", "request_method", "client_ip", "request_query", "request_body"]:
                continue

            if isinstance(value, (str, int, float, bool, type(None))):
                message["attachments"][0]["fields"].append({"title": key, "value": str(value), "short": True})

        # 트레이스백 정보 추가
        if traceback_text := exception_data.get("traceback"):
            # 긴 트레이스백은 잘라서 코드 블록으로 추가
            lines = traceback_text.split("\n")
            if len(lines) > 50:
                lines = lines[:50] + ["..."]

            traceback_block = "\n".join(lines)
            message["attachments"].append(
                {"color": "#7b0000", "title": "Traceback", "text": f"```{traceback_block}```", "mrkdwn_in": ["text"]}
            )

        return message

    # logging.Handler를 상속받아 구현해야 하는 메서드
    def handleError(self, record):
        """로깅 오류 처리"""
        # 핸들러 내부 오류는 기본 로깅으로 처리하지 않고 단순 출력
        if hasattr(self, "handleError") and self.handleError is not logging.Handler.handleError:
            print("SlackHandler error occurred")
            if record.exc_info:
                print(f"Error: {record.exc_info[1]}")
            else:
                print("Handler error (no exception info available)")
