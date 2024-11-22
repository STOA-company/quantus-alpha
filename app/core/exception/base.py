class CustomException(Exception):
    def __init__(self, message: str, status_code: int, error_code: str = None, extra: dict = None):
        self.message = message
        self.status_code = status_code
        self.error_code = error_code
        self.extra = extra or {}
        super().__init__(message)
