from app.core.exception.base import CustomException


class AuthException(CustomException):
    """인증 관련 기본 예외 클래스"""

    pass


class TokenExpiredException(AuthException):
    def __init__(self):
        super().__init__(message="토큰이 만료되었습니다", status_code=401, error_code="TOKEN_EXPIRED")


class InvalidTokenException(AuthException):
    def __init__(self):
        super().__init__(message="유효하지 않은 토큰입니다", status_code=401, error_code="INVALID_TOKEN")


class UserException(CustomException):
    """사용자 관련 기본 예외 클래스"""

    pass


class UserNotFoundException(UserException):
    def __init__(self, user_id: str = None):
        super().__init__(
            message="사용자를 찾을 수 없습니다",
            status_code=404,
            error_code="USER_NOT_FOUND",
            extra={"user_id": user_id} if user_id else None,
        )


class UserAlreadyExistsException(UserException):
    def __init__(self, email: str = None):
        super().__init__(
            message="이미 존재하는 사용자입니다",
            status_code=409,
            error_code="USER_ALREADY_EXISTS",
            extra={"email": email} if email else None,
        )


class FinancialException(CustomException):
    """금융 관련 기본 예외 클래스"""

    pass


class NoFinancialDataException(FinancialException):
    def __init__(self, ticker: str):
        super().__init__(message="해당 종목에 대한 데이터가 없습니다", status_code=404, error_code="NO_DATA")


class InvalidCountryException(FinancialException):
    def __init__(self, country: str):
        super().__init__(message="유효하지 않은 국가 코드입니다", status_code=400, error_code="INVALID_COUNTRY")


class InvalidTickerException(FinancialException):
    def __init__(self, ticker: str):
        super().__init__(message="유효하지 않은 종목 코드입니다", status_code=400, error_code="INVALID_TICKER")


class DataNotFoundException(FinancialException):
    """데이터를 찾을 수 없는 경우의 예외"""

    def __init__(self, ticker: str, data_type: str):
        super().__init__(
            message=f"{ticker} 종목에 대한 {data_type} 데이터가 존재하지 않습니다.",
            status_code=404,
            error_code="DATA_NOT_FOUND",
        )


class AnalysisException(FinancialException):
    """분석 중 발생하는 예외"""

    def __init__(self, analysis_type: str, detail: str):
        super().__init__(
            message=f"{analysis_type} 분석 중 오류가 발생했습니다: {detail}", status_code=500, error_code="ANALYSIS_ERROR"
        )
