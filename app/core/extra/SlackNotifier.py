import socket
import traceback
from typing import Dict, List, Union

import psutil
import requests

from app.core.config import settings


class SlackNotifier:
    SLACK_USER_IDS = {"고경민": "U08011KHGJK", "김광윤": "U089KGFM9CG"}

    def __init__(self, webhook_url=None, mention_ids: Union[list[str], None] = None):
        if not webhook_url:  # DEFAULT WEBHOOK URL SETTING
            webhook_url = "https://hooks.slack.com/services/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"
        self.webhook_url = webhook_url
        self.mention_ids = mention_ids if mention_ids is not None else []
        self.server_info = self.__get_server_info()

    def __get_server_info(self):
        hostname = socket.gethostname()
        try:
            ip_address = socket.gethostbyname(hostname)
        except:  # noqa
            ip_address = "Unknown"
        return f"🖥️ Server: {hostname} ({ip_address})"

    def __get_mention_tags(self, user_name=None) -> str:
        # SLACK_USER_IDS에서 멘션할 사용자 필터링
        mentions = [
            f"<@{user_id}>" for name, user_id in self.SLACK_USER_IDS.items() if user_name is None or name == user_name
        ]

        # mention_ids에서 추가 멘션
        mentions.extend(f"<@{user_id}>" for user_id in self.mention_ids if user_id not in self.SLACK_USER_IDS.values())

        return " ".join(mentions)

    def send_message(self, message, color="#36a64f"):
        full_message = f"{self.server_info}\n\n{message}"
        payload = {"attachments": [{"color": color, "text": full_message}]}
        response = requests.post(self.webhook_url, json=payload)
        return response.status_code == 200

    def notify_success(self, message):
        return self.send_message(f"✅ SUCCESS: {message}", color="#36a64f")

    def notify_info(self, message):
        return self.send_message(f"ℹ️ INFO: {message}", color="#3498db")

    def notify_error(self, error, user_name=None):
        mentions = self.__get_mention_tags(user_name)
        error_message = f"{mentions}\n❌ ERROR:\n```\n{str(error)}\n\n{traceback.format_exc()}```"
        return self.send_message(error_message, color="#ff0000")

    def notify_program_status(self, status):
        return self.send_message(f"🔄 STATUS: {status}", color="#f39c12")

    def _get_memory_usage(self) -> Dict[str, float]:
        """시스템 메모리 사용량을 가져옵니다."""
        memory = psutil.virtual_memory()
        return {
            "total": round(memory.total / (1024**3), 2),  # GB
            "available": round(memory.available / (1024**3), 2),  # GB
            "used": round(memory.used / (1024**3), 2),  # GB
            "percent": memory.percent,
        }

    def _get_memory_alert_message(self, percent: float) -> str:
        """메모리 사용률에 따른 경고 메시지를 반환합니다."""
        if percent >= 90:
            return "⛔ *위험*: 메모리 사용률이 매우 높습니다!"
        elif percent >= 80:
            return "⚠️ *주의*: 메모리 사용률이 높아지고 있습니다."
        return "✅ 메모리 사용률이 정상입니다."

    def notify_memory_status(self, mention_on_high_usage=True):
        """메모리 상태를 슬랙으로 전송합니다."""
        memory_info = self._get_memory_usage()
        alert_message = self._get_memory_alert_message(memory_info["percent"])

        # 높은 메모리 사용률일 때 멘션 추가
        mentions = ""
        if mention_on_high_usage and memory_info["percent"] >= 80:
            mentions = self.__get_mention_tags() + "\n"

        message = (
            f"{mentions}📊 *메모리 모니터링 보고*\n"
            f"총 메모리: {memory_info['total']} GB\n"
            f"사용 중: {memory_info['used']} GB\n"
            f"사용 가능: {memory_info['available']} GB\n"
            f"사용률: {memory_info['percent']}%\n"
            f"{alert_message}"
        )

        color = "#ff0000" if memory_info["percent"] >= 90 else "#f39c12" if memory_info["percent"] >= 80 else "#36a64f"
        return self.send_message(message, color=color)

    def notify_report_post(self, post_id: int, user_id: int, report_items: List[str]):
        """게시글 신고 알림을 슬랙으로 전송합니다."""

        if settings.ENV == "staging":
            base_url = "https://www.alphafinder.dev"
        else:
            base_url = "https://develop.alphafinder.dev"
        message = "📝 *게시글 신고 알림*\n"
        message += f"`게시글 ID`: {post_id}\n"
        message += f"`신고 항목`: {', '.join(report_items)}\n"
        message += f"`신고자`: {user_id}\n"
        message += f"`게시글 링크`: <{base_url}/ko/community/{post_id}|게시글 바로가기>"
        return self.send_message(message, color="#f39c12")
