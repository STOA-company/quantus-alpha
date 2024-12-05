#!/bin/bash

# 스크립트 실행 중 오류 발생시 즉시 중단
set -e

echo "Deploying application..."

# 현재 실행 중인 컨테이너 중지 및 제거
echo "Stopping and removing containers..."
docker-compose down

# 사용하지 않는 이미지 제거
echo "Cleaning up unused images..."
docker image prune -f

# 새로운 이미지 빌드 및 컨테이너 시작
echo "Building and starting containers..."
docker-compose up --build -d

echo "Deployment completed successfully!"
