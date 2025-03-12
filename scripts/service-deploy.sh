set -e

ENVIRONMENT=${1:-dev}

case $ENVIRONMENT in
    prod|production)
        ENV_FILE=.env.prod
        ENV=prod
        BRANCH=main
        ;;
    stage|staging)
        ENV_FILE=.env.stage
        ENV=stage
        BRANCH=staging
        ;;
    dev|development)
        ENV_FILE=.env.dev
        ENV=dev
        BRANCH=dev
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
# echo "Fetching latest changes..."
# git fetch origin || exit 1
# git checkout $BRANCH || exit 1
# git pull origin $BRANCH || exit 1

# Poetry install
echo "Installing dependencies with Poetry..."
poetry install || { echo "Poetry installation failed!"; exit 1; }

# Git 작업 직후, 배포 전 Docker 시스템 정리
echo "Cleaning up Docker system..."
# 중지된 컨테이너 정리
docker container prune -f
# 사용하지 않는 이미지 정리
docker image prune -f
# 사용하지 않는 빌드 캐시 정리
docker builder prune -f
# 현재 디스크 사용량 표시
echo "Current disk usage:"
df -h | grep "/$"

# 만약 디스크 사용률이 85% 이상이면 더 적극적인 정리 수행
disk_usage=$(df -h | grep "/$" | awk '{print $5}' | sed 's/%//')
if [ -n "$disk_usage" ] && [ "$disk_usage" -gt 85 ]; then
    echo "High disk usage detected: ${disk_usage}%. Performing aggressive cleanup..."
    # 더 적극적인 이미지 정리 (사용중이지 않은 모든 이미지)
    docker image prune -a -f
    # 사용하지 않는 볼륨 정리
    docker volume prune -f
    # Docker 시스템 전체 정리
    docker system prune -f
    echo "After aggressive cleanup:"
    df -h | grep "/$"
fi

current_service=$(docker-compose -f docker-compose.yml ps | grep -E 'web-(blue|green)' | grep "Up" | awk '{print $1}' | head -n1)

if [ -z "$current_service" ]; then
    if grep -q "web-blue" /etc/nginx/conf.d/default.conf; then
        current_service="web-blue"
    elif grep -q "web-green" /etc/nginx/conf.d/default.conf; then
        current_service="web-green"
    else
        # 기본값
        current_service="web-blue"
    fi
fi

if [ "$current_service" = "web-blue" ] || [ -z "$current_service" ]; then
    target_service="web-green"
    idle_service="web-blue"
else
    target_service="web-blue"
    idle_service="web-green"
fi

echo "Current active service: $current_service"
echo "Target service for deployment: $target_service"

update_nginx_upstream() {
    local service=$1

    echo "Updating NGINX upstream to point to $service..."

    cat > ./nginx/conf.d/default.conf << EOF
server {
    listen 80;

    location / {
        proxy_pass http://${service}:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    location /health-check {
        proxy_pass http://${service}:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
    }
}
EOF

    docker-compose exec -T nginx nginx -s reload || {
        echo "Failed to reload NGINX. Attempting to restart..."
        docker-compose restart nginx
    }
}

echo "Preparing deployment for $target_service..."

echo "Removing existing container for $target_service if it exists..."
docker-compose rm -f $target_service

echo "Building and starting new $target_service container..."
ENV=$ENV docker-compose -f docker-compose.yml --env-file $ENV_FILE up -d --no-deps --build $target_service

echo "Waiting for container to initialize..."
sleep 10

echo "Performing health check on $target_service..."
max_attempts=12
attempt=1

while [ $attempt -le $max_attempts ]; do
    echo "Health check attempt $attempt of $max_attempts..."

    # 컨테이너 내부에서 직접 헬스 체크
    health_status=$(docker-compose exec -T $target_service curl -s -o /dev/null -w "%{http_code}" "http://localhost:8000/health-check" 2>/dev/null || echo "000")

    if [ "$health_status" = "200" ]; then
        echo "$target_service is healthy and ready!"
        break
    fi

    echo "$target_service is not ready yet (status: $health_status). Waiting 5 seconds..."
    sleep 5
    attempt=$((attempt + 1))
done

if [ $attempt -gt $max_attempts ]; then
    echo "Health check failed after $max_attempts attempts. Reverting to previous setup."
    exit 1
fi

update_nginx_upstream $target_service

echo "Traffic switched to $target_service. Waiting 10 seconds to ensure stability..."
sleep 10

echo "Stopping old $idle_service container..."
docker-compose stop $idle_service

echo "Blue-Green deployment completed successfully!"
echo "Active service is now: $target_service"
