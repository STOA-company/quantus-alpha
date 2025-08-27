# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 프로젝트 개요

Quantus Alpha는 Python으로 작성된 FastAPI 기반의 금융 데이터 API입니다. 주식 시장 데이터, 금융 분석, 투자 도구에 대한 엔드포인트를 제공합니다. 애플리케이션은 다양한 금융 상품(주식, ETF, 배당금, 뉴스 등)에 대한 별도 모듈을 가진 모듈식 아키텍처를 사용합니다.

## 환경 설정

프로젝트는 종속성 관리를 위해 Poetry를 사용하며 여러 환경을 지원합니다:
- `dev`: 개발 환경
- `stage`: 스테이징 환경  
- `prod`: 프로덕션 환경
- `batch`: 배치 처리 환경

환경 구성은 `.env.{ENV}` 파일을 통해 처리됩니다.

## 개발 명령어

### 로컬 개발 환경 설정
```bash
# 의존성 설치
poetry install

# 개발 서버 시작 (Docker를 통한 Redis 포함)
./scripts/dev.sh

# 대안: Poetry로 수동 시작
poetry shell
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Docker 개발
```bash
# 이미지 빌드
docker-compose build

# 모든 서비스 시작
docker-compose up -d

# 특정 서비스 시작
docker-compose up -d web-blue redis

# 로그 확인
docker-compose logs -f --tail=100 web-blue

# 서비스 중지
docker-compose down
```

### 코드 품질
```bash
# Ruff로 린트
poetry run ruff check .

# Ruff로 포맷
poetry run ruff format .
```

### 테스트
```bash
# 테스트 실행
poetry run pytest tests/

# 특정 테스트 실행
poetry run pytest tests/test_app.py
```

### 데이터베이스 관리
```bash
# Alembic 마이그레이션 실행 (데이터 스키마)
alembic -c alembic/data_schema/alembic.ini upgrade head

# Alembic 마이그레이션 실행 (서비스 스키마)
alembic -c alembic/service_schema/alembic.ini upgrade head
```

## 아키텍처 개요

### 핵심 구조
- **`app/main.py`**: 미들웨어, CORS, 모니터링 설정이 포함된 FastAPI 애플리케이션 진입점
- **`app/api/`**: API 라우터 및 버전 관리 (v1, v2)
- **`app/modules/`**: 도메인별로 구성된 기능별 모듈
- **`app/core/`**: 핵심 설정, 의존성, 예외 처리, 로깅
- **`app/database/`**: 데이터베이스 연결 및 CRUD 작업
- **`app/models/`**: 다양한 도메인의 SQLAlchemy 모델
- **`app/utils/`**: 유틸리티 함수 및 헬퍼

### 주요 모듈
- **`chat/`**: LLM 기반 채팅 기능
- **`screener/`**: 주식 및 ETF 스크리닝 도구
- **`community/`**: 커뮤니티 기능 및 소셜 데이터
- **`disclosure/`**: 기업 공시 정보
- **`news/`**: 금융 뉴스 집계
- **`price/`**: 가격 데이터 및 시장 정보
- **`user/`**: 사용자 관리 및 인증

### 데이터베이스 구성
애플리케이션은 여러 MySQL 데이터베이스를 사용합니다:
- 메인 데이터 데이터베이스 (시장 데이터, 가격 등)
- 서비스 데이터베이스 (애플리케이션별 데이터)
- 사용자 데이터베이스 (인증, 사용자 프로필)

데이터베이스 연결은 `app/core/config.py`에서 환경별로 구성됩니다.

### 미들웨어 및 모니터링
- **속도 제한**: API 엔드포인트용 커스텀 속도 제한기
- **OpenTelemetry**: 요청 모니터링용 Zipkin 트레이싱
- **Prometheus**: 모니터링용 메트릭 수집
- **Slack 알림**: 오류 보고 및 알림
- **CORS**: 여러 프론트엔드 도메인에 대해 구성됨

### 인증
- Google OAuth 통합을 통한 JWT 기반 인증
- 전용 사용자 데이터베이스를 통한 사용자 관리
- 보호된 엔드포인트는 인증을 위해 의존성 주입 사용

### 백그라운드 처리
- **Celery**: 비동기 작업 처리
- **Redis**: 메시지 브로커 및 캐싱
- **배치 작업**: 예약된 데이터 업데이트를 위해 `app/batches/`에 위치

### Blue-Green 배포
Docker 설정은 다음과 같은 blue-green 배포를 지원합니다:
- `web-blue`: 기본 애플리케이션 컨테이너
- `web-green`: 무중단 배포를 위한 보조 컨테이너
- 트래픽 라우팅을 위한 NGINX 로드 밸런서

### 로깅
다음을 포함한 포괄적인 로깅 시스템:
- 일별 로테이션을 통한 파일 기반 로깅
- 별도 오류 로그
- 오류 알림을 위한 Slack 통합
- 환경별 로그 레벨

## 개발 가이드라인

### 모듈 구성
새 기능을 생성할 때 기존 모듈 패턴을 따르세요:
```
app/modules/{feature}/
├── __init__.py
├── router.py      # FastAPI 라우트
├── schemas.py     # Pydantic 모델
├── services.py    # 비즈니스 로직
└── models.py      # 데이터베이스 모델 (필요한 경우)
```

### 데이터베이스 모델
- 모델은 `app/models/models_{domain}.py`에서 도메인별로 구성됩니다
- 비동기 지원과 함께 SQLAlchemy 사용
- 기존 명명 규칙을 따르세요

### API 버전 관리
- v1 API는 유지보수 모드입니다
- 새로운 기능은 v2 API를 사용해야 합니다
- `app/modules/{feature}/v2/`의 버전별 라우터

### 환경 변수
모든 구성은 환경 변수를 통해 관리됩니다:
- 데이터베이스 연결
- 외부 API 키 (KIS, Google 등)
- Redis 구성
- 기능 플래그

### 오류 처리
- `app/core/exception/`의 커스텀 예외 클래스
- 프로덕션 오류에 대한 Slack 알림
- 디버깅을 위한 포괄적인 로깅

### 코드 스타일
- 특정 무시 규칙과 함께 린팅용 Ruff 구성
- 줄 길이: 122자
- 임포트 및 포맷팅에 대한 기존 패턴을 따르세요