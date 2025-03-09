# Build stage
FROM python:3.12.2-slim-bookworm AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=2.0.1

WORKDIR /app

# Git 설치 및 ODBC 라이브러리 설치 추가
RUN apt-get update && apt-get install -y git unixodbc unixodbc-dev && rm -rf /var/lib/apt/lists/*

RUN pip install "poetry==$POETRY_VERSION"

# pyproject.toml과 poetry.lock 파일을 먼저 복사
COPY pyproject.toml poetry.lock ./

# Poetry 설정 및 의존성 설치
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

# 전체 소스 코드를 복사하고 서브모듈 초기화
COPY . .
RUN git submodule update --init --recursive

# Runtime stage
FROM python:3.12.2-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/usr/local/bin:/root/.local/bin:$PATH"

WORKDIR /app

# 런타임 스테이지에도 ODBC 라이브러리와 curl 설치 추가
RUN apt-get update && apt-get install -y unixodbc curl && rm -rf /var/lib/apt/lists/*

# 빌더 스테이지에서 설치된 패키지들과 소스 코드를 복사
COPY --from=builder /app /app
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages

RUN pip install --no-cache-dir gunicorn uvicorn

# 환경 변수로 PORT 설정 가능하도록 수정
ENV PORT=8000

CMD ["sh", "-c", "gunicorn --worker-class uvicorn.workers.UvicornWorker --workers 4 --bind 0.0.0.0:${PORT} app.main:app"]
