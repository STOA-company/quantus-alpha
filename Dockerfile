# Build stage
FROM python:3.11-slim-buster AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.8.3

WORKDIR /app

# Git 설치 추가
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

RUN pip install "poetry==$POETRY_VERSION"

# 전체 소스 코드를 먼저 복사 (Git 히스토리 포함)
COPY . .

# 서브모듈 초기화 및 업데이트
RUN git submodule update --init --recursive

COPY pyproject.toml poetry.lock ./
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes

# Runtime stage
FROM python:3.11-slim-buster

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY --from=builder /app/requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# 빌드 스테이지에서 서브모듈이 포함된 전체 소스 코드 복사
COPY --from=builder /app /app

# Uvicorn command for FastAPI
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
