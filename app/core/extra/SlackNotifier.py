import requests
import traceback
import socket

class SlackNotifier:
    def __init__(self, webhook_url=None):
        if not webhook_url:  # DEFAULT WEBHOOK URL SETTING
            webhook_url = "https://hooks.slack.com/services/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"
        self.webhook_url = webhook_url
        self.server_info = self.__get_server_info()

    def __get_server_info(self):
        hostname = socket.gethostname()
        try:
            ip_address = socket.gethostbyname(hostname)
        except:
            ip_address = "Unknown"
        return f"🖥️ Server: {hostname} ({ip_address})"

    def send_message(self, message, color="#36a64f"):
        full_message = f"{self.server_info}\n\n{message}"
        payload = {
            "attachments": [
                {
                    "color": color,
                    "text": full_message
                }
            ]
        }
        response = requests.post(self.webhook_url, json=payload)
        return response.status_code == 200

    def notify_success(self, message):
        return self.send_message(f"✅ SUCCESS: {message}", color="#36a64f")

    def notify_info(self, message):
        return self.send_message(f"ℹ️ INFO: {message}", color="#3498db")

    def notify_error(self, error):
        error_message = f"❌ ERROR:\n```\n{str(error)}\n\n{traceback.format_exc()}```"
        return self.send_message(error_message, color="#ff0000")

    def notify_program_status(self, status):
        return self.send_message(f"🔄 STATUS: {status}", color="#f39c12")