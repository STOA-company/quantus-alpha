FROM python:3.12.2-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=2.0.1 \
    POETRY_CACHE_DIR=/var/cache/poetry \
    PIP_CACHE_DIR=/root/.cache/pip \
    PATH="/root/.local/bin:$PATH"

WORKDIR /app

RUN apt-get update && \
    apt-get install -y git unixodbc unixodbc-dev curl gnupg gcc python3-dev \
    libpango-1.0-0 libpangoft2-1.0-0 libgdk-pixbuf2.0-0 libffi-dev \
    fonts-liberation fonts-dejavu-core fonts-noto-cjk && \
    rm -rf /var/lib/apt/lists/* && \
    pip install "poetry==$POETRY_VERSION" gunicorn uvicorn && \
    mkdir -p $POETRY_CACHE_DIR $PIP_CACHE_DIR && \
    chmod 777 $POETRY_CACHE_DIR $PIP_CACHE_DIR

COPY pyproject.toml poetry.lock ./

RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi --no-root && \
    pip install --no-cache-dir fastapi==0.115.6 pydantic==2.10.4 && \
    python -c "import fastapi; print(f'FastAPI version: {fastapi.__version__}')"

COPY . .


CMD ["sh", "-c", "gunicorn --worker-class uvicorn.workers.UvicornWorker --workers 4 --bind 0.0.0.0:${PORT} app.main:app"]
