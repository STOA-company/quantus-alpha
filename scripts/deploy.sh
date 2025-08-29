#!/bin/bash

set -e

# 환경 변수 설정
PROJECT_DIR="./quantus-alpha"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.yml"
NGINX_CONF="$PROJECT_DIR/nginx.conf"
IMAGE_TAG="${1:-latest}"

echo "🚀 Starting Blue-Green deployment with image tag: $IMAGE_TAG"

# 프로젝트 디렉토리로 이동
cd $PROJECT_DIR

# 최신 이미지 강제 pull
echo "📥 Pulling latest image with tag: $IMAGE_TAG"
export GITHUB_REPOSITORY_OWNER=${GITHUB_REPOSITORY_OWNER:-stoa-company}
export IMAGE_TAG=${IMAGE_TAG}
docker pull ghcr.io/${GITHUB_REPOSITORY_OWNER}/quantus-alpha-dev:${IMAGE_TAG}

# 기본 서비스들 먼저 시작 (redis, nginx, zipkin)
echo "🔧 Starting base services (redis, nginx, zipkin)..."
docker compose up -d redis nginx zipkin

# Redis 서비스가 healthy 상태가 될 때까지 대기
echo "⏳ Waiting for Redis service to be healthy..."
for i in {1..30}; do
    if docker compose ps --services --filter "status=running" | grep -q redis; then
        echo "✅ Redis service is running"
        break
    fi
    echo "⏳ Waiting for Redis service... ($i/30)"
    sleep 5
done

# 현재 활성 컨테이너 확인
CURRENT_ACTIVE=$(docker ps --filter "name=quantus-alpha-" --filter "status=running" --format "{{.Names}}" | grep -E "(blue|green)" | head -1)

if [[ "$CURRENT_ACTIVE" == *"blue"* ]]; then
    CURRENT="blue"
    NEW="green"
    NEW_PORT=8001
else
    CURRENT="green"
    NEW="blue"  
    NEW_PORT=8000
fi

echo "📍 Current active: $CURRENT, Deploying to: $NEW"

# 1. Green 컨테이너 시작 (또는 Blue로 전환)
echo "🟢 Starting $NEW container..."
if [ "$NEW" == "green" ]; then
    docker compose --profile manual up -d quantus-alpha-green
else
    docker compose up -d quantus-alpha-blue
fi

# 2. 컨테이너 헬스체크 대기
echo "🔍 Waiting for $NEW container to be healthy..."
for i in {1..30}; do
    # 컨테이너가 실행 중이고 헬스체크가 성공하는지 확인
    if [ "$(docker inspect --format='{{.State.Status}}' quantus-alpha-$NEW 2>/dev/null)" = "running" ]; then
        # 헬스체크 엔드포인트 호출
        if docker compose exec -T quantus-alpha-$NEW curl -f http://localhost:$NEW_PORT/health-check > /dev/null 2>&1; then
            echo "✅ $NEW container is healthy"
            break
        else
            echo "⚠️ $NEW container is running but health check failed"
        fi
    else
        echo "⚠️ $NEW container is not running yet"
    fi
    echo "⏳ Waiting for $NEW container to be healthy... ($i/30)"
    sleep 10
done

# 3. 헬스체크 실패시 롤백
if ! docker compose exec -T quantus-alpha-$NEW curl -f http://localhost:$NEW_PORT/health-check > /dev/null 2>&1; then
    echo "❌ $NEW container health check failed, rolling back..."
    docker compose logs quantus-alpha-$NEW
    docker compose stop quantus-alpha-$NEW
    docker compose rm -f quantus-alpha-$NEW
    exit 1
fi

# 4. Nginx 설정 업데이트 (Blue/Green 전환)
echo "🔄 Switching nginx to $NEW..."
if [ "$NEW" == "green" ]; then
    # Blue -> Green
    sed -i 's/server quantus-alpha-blue:8000;/server quantus-alpha-green:8001;/' $NGINX_CONF
else
    # Green -> Blue  
    sed -i 's/server quantus-alpha-green:8001;/server quantus-alpha-blue:8000;/' $NGINX_CONF
fi

# 5. Nginx 재시작 (설정 변경 적용)
echo "♻️ Restarting nginx to apply new configuration..."
docker compose restart nginx

# 6. 최종 헬스체크 (nginx를 통한 확인)
echo "🔍 Final health check..."
sleep 5
if curl -f http://localhost/health-check > /dev/null 2>&1; then
    echo "✅ Deployment successful!"
    
    # 7. 이전 컨테이너 정리
    echo "🧹 Cleaning up old $CURRENT container..."
    docker compose stop quantus-alpha-$CURRENT
    docker compose rm -f quantus-alpha-$CURRENT
    
    echo "🎉 Blue-Green deployment completed successfully!"
else
    echo "❌ Final health check failed, manual intervention required"
    exit 1
fi