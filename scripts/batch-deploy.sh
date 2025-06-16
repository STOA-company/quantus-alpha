set -e

ENVIRONMENT=${1:-batch}

ENV_FILE=.env.batch
ENV=batch
BRANCH=dev
COMPOSE_FILE="docker-compose.batch.yml"
RUN_CELERY=true

echo "Deploying batch server for environment: $ENV using $ENV_FILE (Branch: $BRANCH)"

echo "Changing to project directory..."
cd ~/quantus-alpha || exit 1

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: $ENV_FILE not found!"
    exit 1
fi


echo "Fetching latest changes..."
git fetch origin || exit 1
git checkout $BRANCH || exit 1
git pull origin $BRANCH || exit 1

echo "Installing dependencies with Poetry..."
poetry install || { echo "Poetry installation failed!"; exit 1; }

echo "Stopping containers with docker compose down..."
ENV=$ENV docker compose -f $COMPOSE_FILE --env-file $ENV_FILE down || true

echo "Cleaning up Docker images..."
docker images | grep 'quantus-backend' | awk '{print $3}' | xargs docker rmi -f || true

echo "Cleaning up Docker system resources..."
docker system prune -f || true

echo "Cleaning up Docker volumes..."
docker volume prune -f || true

echo "Cleaning up Docker builder cache..."
docker builder prune -f || true

echo "Building and starting new batch containers..."
ENV=$ENV docker compose -f $COMPOSE_FILE --env-file $ENV_FILE up --build -d

echo "Waiting for containers to start..."
sleep 10

echo "Starting health checks..."
max_attempts=12
attempt=1

while [ $attempt -le $max_attempts ]; do
    echo "Health check attempt $attempt of $max_attempts..."

    celery_status=$(docker compose -f $COMPOSE_FILE exec -T celery_worker celery -A app.celery_worker status 2>/dev/null | grep "OK" || echo "")
    rabbitmq_status=$(docker compose -f $COMPOSE_FILE exec -T rabbitmq rabbitmq-diagnostics -q ping 2>/dev/null || echo "")
    redis_status=$(docker compose -f $COMPOSE_FILE exec -T redis redis-cli ping 2>/dev/null || echo "")

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

echo "Deployment failed after $max_attempts attempts"
exit 1
