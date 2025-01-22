from typing import Union
import requests
import traceback
import socket


class SlackNotifier:
    SLACK_USER_IDS = {"고경민": "U08011KHGJK"}

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

    def __get_mention_tags(self) -> str:
        mentions = []
        # SLACK_USER_IDS의 모든 사용자를 멘션
        for name, user_id in self.SLACK_USER_IDS.items():
            mentions.append(f"<@{user_id}>")

        for user_id in self.mention_ids:
            if user_id not in self.SLACK_USER_IDS:
                mentions.append(f"@{user_id}")

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

    def notify_error(self, error):
        mentions = self.__get_mention_tags()
        error_message = f"{mentions}\n❌ ERROR:\n```\n{str(error)}\n\n{traceback.format_exc()}```"
        return self.send_message(error_message, color="#ff0000")

    def notify_program_status(self, status):
        return self.send_message(f"🔄 STATUS: {status}", color="#f39c12")
