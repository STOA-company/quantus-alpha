from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import logging
from typing import Callable, Dict, Any, Optional, List
from app.core.exception.base import CustomException

# 아래와 같이 수정: 직접 SlackNotifier 클래스를 가져오도록 함
from app.core.extra.SlackNotifier import SlackNotifier

logger = logging.getLogger(__name__)


class SlackExceptionMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: FastAPI,
        webhook_url: Optional[str] = None,
        mention_ids: Optional[List[str]] = None,
        mention_usernames: Optional[List[str]] = None,
        include_traceback: bool = True,
        include_request_body: bool = False,
        error_status_codes: List[int] = None,
        environment: str = "production",
        notify_environments: Optional[List[str]] = None,
    ):
        super().__init__(app)
        self.slack_notifier = SlackNotifier(webhook_url=webhook_url, mention_ids=mention_ids)
        self.include_traceback = include_traceback
        self.include_request_body = include_request_body
        self.error_status_codes = error_status_codes or [500]
        self.environment = environment
        self.mention_usernames = mention_usernames or []
        self.notify_environments = notify_environments or ["stage", "prod", "production"]

    async def dispatch(self, request: Request, call_next: Callable) -> JSONResponse:
        try:
            response = await call_next(request)

            # 설정된 에러 상태 코드 발생 시에도 슬랙 알림 전송
            if response.status_code in self.error_status_codes and self.environment in self.notify_environments:
                await self._send_slack_notification(
                    request=request,
                    status_code=response.status_code,
                    exception=None,
                    message=f"Error response with status code: {response.status_code}",
                )

            return response

        except CustomException as exc:
            # 커스텀 예외 로깅
            logger.error(f"CustomException: {exc.message}", exc_info=True)

            # 특정 환경에서만 슬랙 알림 전송
            if self.environment in self.notify_environments:
                await self._send_slack_notification(
                    request=request,
                    status_code=exc.status_code,
                    exception=exc,
                    message=f"{exc.error_code or 'ERROR'}: {exc.message}",
                    extra_data=exc.extra,
                    exception_type="CustomException",
                )

            # 예외를 다시 발생시켜 FastAPI의 기본 예외 처리기가 처리하도록 함
            raise

        except Exception as exc:
            # 일반 예외 로깅
            logger.exception(f"Unhandled exception: {exc}")

            # 특정 환경에서만 슬랙 알림 전송
            if self.environment in self.notify_environments:
                await self._send_slack_notification(
                    request=request,
                    status_code=500,
                    exception=exc,
                    message=f"Unhandled exception: {str(exc)}",
                    exception_type=type(exc).__name__,
                )

            # 예외를 다시 발생시켜 FastAPI의 기본 예외 처리기가 처리하도록 함
            raise

    async def _send_slack_notification(
        self,
        request: Request,
        status_code: int,
        exception: Optional[Exception] = None,
        message: Any = None,
        extra_data: Optional[Dict[str, Any]] = None,
        exception_type: str = "Exception",
    ) -> None:
        """슬랙으로 에러 알림을 전송합니다."""
        try:
            # 요청 정보 수집
            method = request.method
            url = str(request.url)
            client_ip = request.client.host if request.client else "Unknown"

            # API 경로 정보 추출 (라우트 정보)
            actual_path = "Unknown actual path"
            scope = request.scope

            # 실제 요청 경로(path parameters 값 포함)
            if "path" in scope:
                actual_path = scope["path"]

            # 요청 본문 (설정된 경우만)
            request_body = None
            if self.include_request_body:
                try:
                    body_bytes = await request.body()
                    try:
                        # JSON으로 파싱 시도
                        request_body = await request.json()
                        request_body = str(request_body)
                    except:  # noqa: E722
                        # 실패하면 텍스트로 처리
                        request_body = body_bytes.decode()
                except Exception:  # noqa: E722
                    request_body = "Could not parse request body"

            # 에러 세부 정보 구성
            error_details = []

            # 기본 정보 추가
            error_details.append(f"*환경*: {self.environment}")
            error_details.append(f"*실제 요청 경로*: {actual_path}")
            error_details.append(f"*상태 코드*: {status_code}")
            error_details.append(f"*요청 메서드*: {method}")
            error_details.append(f"*URL*: {url}")
            error_details.append(f"*클라이언트 IP*: {client_ip}")

            # CustomException 추가 정보
            if isinstance(exception, CustomException) and exception.extra:
                error_details.append(f"*추가 정보*: {str(exception.extra)}")
            # 일반 추가 정보
            elif extra_data:
                error_details.append(f"*추가 정보*: {str(extra_data)}")

            # 에러 본문 정보 추가 (설정된 경우만)
            if request_body:
                error_details.append(f"*요청 본문*:\n```{request_body}```")

            # 에러 메시지 처리
            if isinstance(message, dict):
                # dict 형태의 응답일 경우 적절히 추출
                if "error" in message:
                    message = message["error"]

                if "errors" in message and not message["errors"]:
                    # errors 필드가 빈 값인 경우 제거
                    message.pop("errors", None)

                # dictionary를 문자열로 변환
                formatted_message = str(message)
            else:
                formatted_message = str(message)

            # 에러 메시지 구성
            error_message = f"❌ {exception_type}: {formatted_message} (경로: {actual_path})\n\n" + "\n".join(
                error_details
            )

            # 특정 사용자에게 멘션하는 경우
            if self.mention_usernames:
                for username in self.mention_usernames:
                    self.slack_notifier.notify_error(error_message, user_name=username)
            else:
                # 일반적인 에러 알림
                self.slack_notifier.notify_error(error_message)

        except Exception as e:
            logger.exception(f"Error in SlackExceptionMiddleware: {e}")
            # 미들웨어 자체의 오류로 인해 애플리케이션 오류가 가려지지 않도록 함


# 미들웨어 사용 예제
def add_slack_middleware(
    app: FastAPI,
    webhook_url: Optional[str] = None,
    mention_ids: Optional[List[str]] = None,
    mention_usernames: Optional[List[str]] = None,
    include_traceback: bool = True,
    include_request_body: bool = False,
    error_status_codes: List[int] = None,
    environment: str = "production",
    notify_environments: Optional[List[str]] = None,
) -> None:
    """
    FastAPI 애플리케이션에 슬랙 알림 미들웨어를 추가합니다.

    Args:
        app: FastAPI 애플리케이션 인스턴스
        webhook_url: 슬랙 웹훅 URL (None이면 SlackNotifier의 기본값 사용)
        mention_ids: 알림을 받을 사용자 ID 목록 (SlackNotifier.SLACK_USER_IDS에 없는 경우)
        mention_usernames: 알림을 받을 사용자 이름 목록 (SlackNotifier.SLACK_USER_IDS에 있는 경우)
        include_traceback: 스택 트레이스 포함 여부
        include_request_body: 요청 본문 포함 여부
        error_status_codes: 알림을 트리거할 상태 코드 목록
        environment: 환경 이름 (prod, stage, dev 등)
        notify_environments: 알림을 보낼 환경 목록 (기본값: ["stage", "prod", "production"])
    """
    app.add_middleware(
        SlackExceptionMiddleware,
        webhook_url=webhook_url,
        mention_ids=mention_ids,
        mention_usernames=mention_usernames,
        include_traceback=include_traceback,
        include_request_body=include_request_body,
        error_status_codes=error_status_codes or [500],
        environment=environment,
        notify_environments=notify_environments,
    )
