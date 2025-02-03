#!/bin/bash
# 스크립트 실행 중 오류 발생시 즉시 중단
set -e

ENVIRONMENT=${1:-dev}

case $ENVIRONMENT in
    prod|production)
        ENV_FILE=.env.prod
        ENV=prod
        BRANCH=main
        RUN_CELERY=false
        ;;
    stage|staging)
        ENV_FILE=.env.stage
        ENV=stage
        BRANCH=staging
        RUN_CELERY=false
        ;;
    dev|development)
        ENV_FILE=.env.dev
        ENV=dev
        BRANCH=dev
        RUN_CELERY=true
        ;;
    batch)
        ENV_FILE=.env.batch
        ENV=batch
        BRANCH=dev  # 배치 서버도 dev 브랜치 사용
        ;;
    *)
        echo "Unknown environment: $ENVIRONMENT"
        exit 1
        ;;
esac

echo "Deploying for environment: $ENV using $ENV_FILE (Branch: $BRANCH)"

# 환경변수 파일 확인
if [ ! -f "$ENV_FILE" ]; then
    echo "Error: $ENV_FILE not found!"
    exit 1
fi

# 프로젝트 디렉토리로 이동
echo "Changing to project directory..."
cd ~/quantus-alpha || exit 1

# Git 작업
echo "Fetching latest changes..."
git fetch origin || exit 1
git checkout $BRANCH || exit 1
git pull origin $BRANCH || exit 1

# Poetry install
echo "Installing dependencies with Poetry..."
poetry install || { echo "Poetry installation failed!"; exit 1; }

# Docker Compose 파일 선택
if [ "$ENV" = "batch" ]; then
    COMPOSE_FILE="docker-compose.batch.yml"
else
    COMPOSE_FILE="docker-compose.yml"
fi

# Docker Compose down
echo "Stopping containers with docker-compose down..."
ENV=$ENV docker-compose -f $COMPOSE_FILE --env-file $ENV_FILE down || true

# Docker 이미지 정리
echo "Cleaning up Docker images..."
docker images | grep 'quantus-backend' | awk '{print $3}' | xargs docker rmi -f || true

# Docker 시스템 리소스 정리
echo "Cleaning up Docker system resources..."
docker system prune -f || true

# Docker 볼륨 정리
echo "Cleaning up Docker volumes..."
docker volume prune -f || true

# Docker 빌더 캐시 정리
echo "Cleaning up Docker builder cache..."
docker builder prune -f || true

# 새 컨테이너 빌드 및 실행
echo "Building and starting new containers..."
ENV=$ENV docker-compose -f $COMPOSE_FILE --env-file $ENV_FILE up --build -d

# 컨테이너 시작 대기
echo "Waiting for containers to start..."
sleep 10

# Health check
echo "Starting health checks..."
max_attempts=12
attempt=1

if [ "$ENV" = "batch" ]; then
    # 배치 서버 health check
    while [ $attempt -le $max_attempts ]; do
        echo "Health check attempt $attempt of $max_attempts..."

        celery_status=$(docker-compose -f $COMPOSE_FILE exec -T celery_worker celery -A app.celery_worker status 2>/dev/null | grep "OK" || echo "")
        rabbitmq_status=$(docker-compose -f $COMPOSE_FILE exec -T rabbitmq rabbitmq-diagnostics -q ping 2>/dev/null || echo "")
        redis_status=$(docker-compose -f $COMPOSE_FILE exec -T redis redis-cli ping 2>/dev/null || echo "")

        if [ ! -z "$celery_status" ] && [ ! -z "$rabbitmq_status" ] && [ "$redis_status" = "PONG" ]; then
            echo "Deployment successful! All batch services are running."
            echo "Celery Status: OK"
            echo "RabbitMQ Status: OK"
            echo "Redis Status: OK"
            exit 0
        fi

        echo "Services not ready yet:"
        echo "- Celery: ${celery_status:-Not Ready}"
        echo "- RabbitMQ: ${rabbitmq_status:-Not Ready}"
        echo "- Redis: ${redis_status:-Not Ready}"

        echo "Waiting 5 seconds..."
        sleep 5
        attempt=$((attempt + 1))
    done
else
    # 웹 서버 health check
    while [ $attempt -le $max_attempts ]; do
        echo "Health check attempt $attempt of $max_attempts..."

        api_status=$(curl -s -o /dev/null -w "%{http_code}" localhost:80/docs || echo "000")

        if [ "$api_status" = "200" ]; then
            echo "Deployment successful! API is running."
            echo "API Status: $api_status"
            exit 0
        fi

        echo "Services not ready yet:"
        echo "- API status: $api_status"

        echo "Waiting 5 seconds..."
        sleep 5
        attempt=$((attempt + 1))
    done
fi

echo "Deployment failed after $max_attempts attempts"
exit 1
