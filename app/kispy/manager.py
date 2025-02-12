from datetime import datetime
import time
import logging

from typing import Optional

from app.kispy.api import KISAPI
from app.kispy.sdk import auth


class KISAPIManager:
    _instance: Optional["KISAPIManager"] = None
    _last_token_refresh: Optional[datetime] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._api = None
        return cls._instance

    def __init__(self):
        if self._api is None:
            self._api = KISAPI(auth=auth)
            self._last_token_refresh = datetime.now()

    def get_api(self) -> KISAPI:
        """API 인스턴스 반환 (토큰 유효성 체크 포함)"""
        current_time = datetime.now()

        if self._last_token_refresh and (current_time - self._last_token_refresh).total_seconds() >= 60:
            if not self._api.is_token_valid():
                time.sleep(60 - (current_time - self._last_token_refresh).total_seconds())
                if self._api.refresh_token():
                    self._last_token_refresh = datetime.now()
                    logging.info("Successfully refreshed token")
                else:
                    logging.error("Failed to refresh token")

        return self._api
