import logging
from typing import Any, Sequence

from app.core.exception.base import CustomException
from fastapi.applications import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.status import (
    HTTP_422_UNPROCESSABLE_ENTITY,
    HTTP_500_INTERNAL_SERVER_ERROR,
)
from app.core.exception.custom import (
    AuthException,
    InvalidCountryException,
    InvalidTickerException,
    NoFinancialDataException,
    UserException,
    TokenExpiredException,
    InvalidTokenException,
    UserNotFoundException,
    UserAlreadyExistsException,
    DataNotFoundException,
    AnalysisException,
)

logger = logging.getLogger(__name__)


def _make_json_resp(
    status_code: int,
    message: str,
    errors: Sequence[Any] | dict | str | None,
    headers: dict[str, Any] | None = None,
) -> JSONResponse:
    content = {"error": {"code": status_code, "message": message, "errors": errors}}
    logger.info(content)
    return JSONResponse(
        status_code=status_code,
        content=jsonable_encoder(content),
        headers=headers,
    )


def _make_detailed_error_response(request: Request, exc: CustomException) -> JSONResponse:
    return _make_json_resp(
        status_code=exc.status_code,
        message=exc.message,
        errors={"code": exc.error_code, "reason": type(exc).__name__, "message": exc.message},
    )


def _make_simple_error_response(request: Request, exc: CustomException) -> JSONResponse:
    return _make_json_resp(status_code=exc.status_code, message=exc.message, errors={})


async def custom_exception_handler(request: Request, exc: CustomException) -> JSONResponse:
    return _make_detailed_error_response(request, exc)


async def user_exception_handler(request: Request, exc: UserException) -> JSONResponse:
    return _make_detailed_error_response(request, exc)


async def token_expired_exception_handler(request: Request, exc: TokenExpiredException) -> JSONResponse:
    return _make_simple_error_response(request, exc)


async def invalid_token_exception_handler(request: Request, exc: InvalidTokenException) -> JSONResponse:
    return _make_simple_error_response(request, exc)


async def user_not_found_exception_handler(request: Request, exc: UserNotFoundException) -> JSONResponse:
    return _make_simple_error_response(request, exc)


async def user_already_exists_exception_handler(request: Request, exc: UserAlreadyExistsException) -> JSONResponse:
    return _make_simple_error_response(request, exc)


async def auth_exception_handler(request: Request, exc: AuthException) -> JSONResponse:
    return _make_detailed_error_response(request, exc)


async def no_financial_data_exception_handler(request: Request, exc: NoFinancialDataException) -> JSONResponse:
    return _make_simple_error_response(request, exc)


async def invalid_country_exception_handler(request: Request, exc: InvalidCountryException) -> JSONResponse:
    return _make_simple_error_response(request, exc)


async def invalid_ticker_exception_handler(request: Request, exc: InvalidTickerException) -> JSONResponse:
    return _make_simple_error_response(request, exc)


async def data_not_found_exception_handler(request: Request, exc: DataNotFoundException) -> JSONResponse:
    return _make_simple_error_response(request, exc)


async def analysis_exception_handler(request: Request, exc: AnalysisException) -> JSONResponse:
    return _make_simple_error_response(request, exc)


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    return _make_json_resp(
        status_code=exc.status_code,
        message=exc.detail,
        errors={},
        headers=getattr(exc, "headers", None),
    )


async def request_validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    return _make_json_resp(
        status_code=HTTP_422_UNPROCESSABLE_ENTITY,
        message="입력값이 잘못되었습니다",
        errors=exc.errors(),
    )


async def sqlalchemy_error_handler(request: Request, exc: SQLAlchemyError) -> JSONResponse:
    logger.error(exc, exc_info=True)
    return _make_json_resp(
        status_code=HTTP_500_INTERNAL_SERVER_ERROR,
        message="서버 오류가 발생했습니다",
        errors={
            "domain": "SQLAlchemyError",
            "reason": type(exc).__name__,
            "message": str(exc),
        },
    )


async def exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error(exc, exc_info=True)
    return _make_json_resp(
        status_code=HTTP_500_INTERNAL_SERVER_ERROR,
        message="서버 오류가 발생했습니다",
        errors=[
            {
                "domain": "Exception",
                "reason": type(exc).__name__,
                "message": str(exc),
            }
        ],
    )


def initialize(app: FastAPI) -> None:
    app.add_exception_handler(CustomException, custom_exception_handler)
    app.add_exception_handler(AuthException, auth_exception_handler)
    app.add_exception_handler(UserException, user_exception_handler)
    app.add_exception_handler(TokenExpiredException, token_expired_exception_handler)
    app.add_exception_handler(InvalidTokenException, invalid_token_exception_handler)
    app.add_exception_handler(UserNotFoundException, user_not_found_exception_handler)
    app.add_exception_handler(UserAlreadyExistsException, user_already_exists_exception_handler)
    app.add_exception_handler(NoFinancialDataException, no_financial_data_exception_handler)
    app.add_exception_handler(InvalidCountryException, invalid_country_exception_handler)
    app.add_exception_handler(InvalidTickerException, invalid_ticker_exception_handler)
    app.add_exception_handler(DataNotFoundException, data_not_found_exception_handler)
    app.add_exception_handler(AnalysisException, analysis_exception_handler)
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, request_validation_exception_handler)
    app.add_exception_handler(SQLAlchemyError, sqlalchemy_error_handler)
    app.add_exception_handler(Exception, exception_handler)
