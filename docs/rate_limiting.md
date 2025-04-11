# 레이트 리미팅 시스템

이 문서는 애플리케이션에 구현된 Redis 기반 레이트 리미팅 시스템에 대해 설명합니다.

## 개요

레이트 리미팅 시스템은 두 가지 보호 계층을 제공합니다:

1. **전역 레이트 리미팅**: 모든 API 엔드포인트에 구성 가능한 제한이 적용됩니다.
2. **엔드포인트별 레이트 리미팅**: 민감한 작업에 대해 더 엄격한 제한을 허용합니다.

이 구현은 요청 수를 효율적으로 분산 추적하기 위해 Redis를 기반으로 하며 정확한 레이트 리미팅을 위해 슬라이딩 윈도우 접근 방식을 사용합니다.

## 기능

- Redis 기반 구현으로 분산 레이트 리미팅 지원
- 시간 윈도우 내에서 정확한 추적을 위한 슬라이딩 윈도우 카운터
- IP 기반 클라이언트 식별(사용자 지정 식별자 지원)
- 레이트 리미트 우회를 위한 화이트리스트 기능
- 커스터마이징 가능한 응답 헤더
- 종합적인 관리 API
- 안정적인 오류 처리 및 대체 메커니즘

## 작동 방식

### 슬라이딩 윈도우 구현

레이트 리미팅은 다음과 같은 슬라이딩 윈도우 접근 방식을 사용합니다:

1. 시간을 개별 윈도우로 나눕니다(예: 60초 윈도우)
2. 클라이언트 ID와 현재 시간 윈도우를 기반으로 Redis 키를 사용합니다
3. 이러한 윈도우 내에서 카운터를 증가시킵니다
4. Redis TTL을 사용하여 오래된 윈도우를 자동으로 만료시킵니다

Redis 키는 다음과 같은 구조로 되어 있습니다:

- **전역 레이트 제한**: `rate:global:CLIENT_ID:WINDOW_ID`
- **엔드포인트 레이트 제한**: `rate:endpoint:PATH:CLIENT_ID:WINDOW_ID`

여기서:
- `CLIENT_ID`는 일반적으로 요청자의 IP 주소입니다
- `WINDOW_ID`는 윈도우 크기로 나눈 타임스탬프입니다
- `PATH`는 엔드포인트별 제한을 위한 엔드포인트 경로입니다

### Redis 키 구조

키 구조는 다음과 같은 목적으로 최적화되어 있습니다:

1. **성능**: 키 조회를 최소화하고 관련 데이터를 함께 유지합니다
2. **확장성**: Redis 핫스팟을 방지하기 위해 키를 균등하게 분산합니다
3. **TTL 관리**: 만료된 윈도우의 자동 정리를 허용합니다

## 설정

### 전역 레이트 리미팅

전역 레이트 리미팅 미들웨어는 `app/main.py`에서 구성됩니다:

```python
app.add_middleware(
    GlobalRateLimitMiddleware,
    max_requests=100,              # 100개 요청의 전역 제한
    window_seconds=60,             # 60초 시간 윈도우
    exclude_paths=["/health-check", "/metrics", ...],  # 제외된 경로
)
```

설정 매개변수:

- `max_requests`: 시간 윈도우 내에 허용되는 최대 요청 수
- `window_seconds`: 시간 윈도우 기간(초)
- `exclude_paths`: 레이트 리미팅에서 제외할 경로 접두사 목록
- `get_client_id`: 클라이언트 식별을 사용자 지정하기 위한 선택적 함수
- `rate_limiter_service`: 선택적 사용자 지정 레이트 리미터 서비스 인스턴스

### 엔드포인트별 레이트 리미팅

더 엄격한 제한이 필요한 엔드포인트의 경우 의존성 주입 패턴을 사용하세요:

```python
from app.middlewares.rate_limiter import endpoint_rate_limiter
from fastapi import Depends, Request

@app.post("/sensitive-operation")
async def sensitive_endpoint(
    request: Request,
    rate_limit: bool = Depends(endpoint_rate_limiter(max_requests=1, window_seconds=10))
):
    # 이 엔드포인트는 전역 레이트 제한 외에도
    # 클라이언트당 10초당 1개의 요청으로 제한됩니다
    return {"message": "민감한 작업이 완료되었습니다"}
```

## 응답 헤더

시스템은 모든 응답에 표준 레이트 리미팅 헤더를 추가합니다:

- `X-RateLimit-Limit`: 현재 윈도우에서 허용되는 최대 요청 수
- `X-RateLimit-Remaining`: 현재 윈도우에서 남은 요청 수
- `X-RateLimit-Reset`: 현재 레이트 제한 윈도우가 재설정될 때까지의 초
- `Retry-After`: 클라이언트가 재시도할 수 있는 시기를 나타내는 `Retry-After` 헤더(429 응답에만 해당)

## 화이트리스트 관리

클라이언트를 화이트리스트에 추가하여 레이트 리미팅에서 제외할 수 있습니다:

### 화이트리스트에 클라이언트 추가

```bash
curl -X POST "http://localhost:8000/admin/rate-limiter/whitelist/127.0.0.1" \
  -H "X-API-Key: your-secret-admin-api-key"
```

### 화이트리스트 보기

```bash
curl "http://localhost:8000/admin/rate-limiter/whitelist" \
  -H "X-API-Key: your-secret-admin-api-key"
```

### 화이트리스트에서 제거

```bash
curl -X DELETE "http://localhost:8000/admin/rate-limiter/whitelist/127.0.0.1" \
  -H "X-API-Key: your-secret-admin-api-key"
```

## 관리 API

레이트 리미터는 레이트 제한을 관리하고 모니터링하기 위한 관리 API를 `/admin/rate-limiter`에 포함하고 있습니다.

이러한 엔드포인트에 접근하려면 `X-API-Key` 헤더에 유효한 API 키를 포함해야 합니다.

### 사용 가능한 엔드포인트:

- `GET /admin/rate-limiter/whitelist` - 모든 화이트리스트 클라이언트 나열
- `POST /admin/rate-limiter/whitelist/{client_id}` - 화이트리스트에 클라이언트 추가
- `DELETE /admin/rate-limiter/whitelist/{client_id}` - 화이트리스트에서 클라이언트 제거
- `GET /admin/rate-limiter/stats` - 레이트 제한 통계 가져오기
- `DELETE /admin/rate-limiter/clear` - 특정 클라이언트 또는 경로에 대한 레이트 제한 지우기

## 오류 처리

클라이언트가 레이트 제한을 초과하면 API는 다음과 같이 응답합니다:

- HTTP 상태 코드 `429 Too Many Requests`
- 오류 세부 정보가 포함된 JSON 응답 본문
- 클라이언트가 재시도할 수 있는 시기를 나타내는 `Retry-After` 헤더

## 엣지 케이스 및 안전장치

- **Redis 장애**: Redis를 사용할 수 없는 경우 시스템은 요청을 허용하는 모드로 전환됩니다
- **사용자 지정 클라이언트 식별**: 토큰, 사용자 ID 등으로 클라이언트를 식별하는 기능 지원
- **단계적 성능 저하**: 시스템은 Redis가 높은 부하 상태에서도 계속 작동하도록 설계되었습니다
