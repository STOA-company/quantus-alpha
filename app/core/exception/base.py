class CustomException(Exception):
    def __init__(self, message: str, status_code: int, error_code: str = None, extra: dict = None):
        self.message = message
        self.status_code = status_code
        self.error_code = error_code
        self.extra = extra or {}
        super().__init__(message)


class DuplicateException(CustomException):
    def __init__(self, message: str, status_code: int = 409, error_code: str = None, extra: dict = None):
        super().__init__(message, status_code, error_code, extra)


class NotFoundException(CustomException):
    def __init__(self, message: str, status_code: int = 404, error_code: str = None, extra: dict = None):
        super().__init__(message, status_code, error_code, extra)
