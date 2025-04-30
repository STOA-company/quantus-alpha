set -e

export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1

ENVIRONMENT=${1:-dev}
CLEAN_CACHE=${2:-false}

case $ENVIRONMENT in
    prod|production)
        ENV_FILE=.env
        ENV=prod
        BRANCH=main
        ;;
    stage|staging)
        ENV_FILE=.env
        ENV=stage
        BRANCH=staging
        ;;
    dev|development)
        ENV_FILE=.env
        ENV=dev
        BRANCH=dev
        ;;
    *)
        echo "Unknown environment: $ENVIRONMENT"
        exit 1
        ;;
esac

echo "Deploying for environment: $ENV using $ENV_FILE (Branch: $BRANCH)"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: $ENV_FILE not found!"
    exit 1
fi

echo "Changing to project directory..."
cd ~/quantus-alpha || exit 1

echo "Fetching latest changes..."
git fetch origin || exit 1
git checkout $BRANCH || exit 1
git pull origin $BRANCH || exit 1

echo "Updating git submodules..."
git submodule update --init --recursive || exit 1

disk_usage=$(df -h | grep "/$" | awk '{print $5}' | sed 's/%//')
if [ -n "$disk_usage" ] && [ "$disk_usage" -gt 85 ]; then
    echo "High disk usage detected: ${disk_usage}%. Performing cleanup..."
    docker container prune -f
    docker image prune -f

    if [ "$disk_usage" -gt 90 ]; then
        echo "Critical disk usage! Performing aggressive cleanup..."
        docker image prune -a -f
        docker volume prune -f
        docker system prune -f
    fi

    echo "After cleanup:"
    df -h | grep "/$"
fi

if [ "$CLEAN_CACHE" = "true" ]; then
    echo "Cleaning pip and poetry caches..."
    docker volume rm -f pip-cache poetry-cache 2>/dev/null || true
    echo "Creating new cache volumes..."
    docker volume create pip-cache
    docker volume create poetry-cache
fi

current_service=$(docker-compose -f docker-compose.yml ps | grep -E 'web-(blue|green)' | grep "Up" | awk '{print $1}' | head -n1)

if [ -z "$current_service" ]; then
    if grep -q "web-blue" /etc/nginx/conf.d/default.conf 2>/dev/null; then
        current_service="web-blue"
    elif grep -q "web-green" /etc/nginx/conf.d/default.conf 2>/dev/null; then
        current_service="web-green"
    else
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

    # 타임아웃 설정 증가
    proxy_connect_timeout 1800s;
    proxy_read_timeout 300s;
    proxy_send_timeout 300s;

    # 버퍼링 설정 비활성화 (스트리밍용)
    proxy_buffering off;

    location /stub_status {
        stub_status on;
        allow 127.0.0.1;
        allow 172.16.0.0/12;
        deny all;
    }

    location / {
        proxy_pass http://${service}:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    # 채팅 스트리밍 엔드포인트 설정
    location /api/v1/chat/stream {
        proxy_pass http://${service}:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header Connection '';
        proxy_http_version 1.1;
        chunked_transfer_encoding off;
        proxy_buffering off;
        proxy_cache off;
        proxy_read_timeout 1800s;  # 스트리밍용 더 긴 타임아웃
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

if docker-compose ps $target_service | grep -q $target_service; then
    echo "Removing existing container for $target_service..."
    docker-compose rm -f $target_service
fi

echo "Building $target_service container..."
docker-compose -f docker-compose.yml build $target_service

echo "Starting $target_service container..."
docker-compose -f docker-compose.yml up -d --no-deps $target_service

echo "Waiting for container to initialize..."
sleep 10

echo "Performing health check on $target_service..."
max_attempts=12
attempt=1

while [ $attempt -le $max_attempts ]; do
    echo "Health check attempt $attempt of $max_attempts..."

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
    echo "Check logs with: docker-compose logs $target_service"
    exit 1
fi

update_nginx_upstream $target_service

echo "Traffic switched to $target_service. Waiting 10 seconds to ensure stability..."
sleep 10

echo "Stopping old $idle_service container..."
docker-compose stop $idle_service

echo "Blue-Green deployment completed successfully!"
echo "Active service is now: $target_service"
