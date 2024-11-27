#!/bin/bash

# 스크립트 실행 중 오류 발생시 즉시 중단
set -e

# 프로젝트 디렉토리로 이동
echo "Changing to project directory..."
cd ~/quantus-backend || exit 1

# Git 작업
echo "Fetching latest changes..."
git fetch origin || exit 1
git checkout main || exit 1
git pull origin main || exit 1

# Docker 컨테이너 정리
echo "Cleaning up Docker containers..."
docker stop nginx web || true
docker rm nginx web || true

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
docker-compose up --build -d

# 컨테이너 시작 대기
echo "Waiting for containers to start..."
sleep 10

# Health check
echo "Starting health checks..."
max_attempts=12  # 최대 60초 대기 (5초 x 12번)
attempt=1

while [ $attempt -le $max_attempts ]; do
    echo "Health check attempt $attempt of $max_attempts..."

    status_code=$(curl -s -o /dev/null -w "%{http_code}" localhost:80/docs || echo "000")

    if [ "$status_code" = "200" ]; then
        echo "Deployment successful! API is responding with 200 status code."
        exit 0
    fi

    echo "API not ready yet (status code: $status_code). Waiting 5 seconds..."
    sleep 5
    attempt=$((attempt + 1))
done

echo "Deployment failed: API did not respond with 200 status code after $max_attempts attempts"
echo "Last status code: $status_code"
exit 1
