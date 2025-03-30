#!/bin/bash
set -e

echo "Starting Redis and required services..."
docker-compose -f docker-compose.local.yml up -d

ENV_FILE=.env
if [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' $ENV_FILE | xargs)
else
    echo "Warning: $ENV_FILE not found. Using default environment variables."
fi

echo "Starting development server..."
poetry run uvicorn app.main:app --reload --reload-dir=app/ --host 0.0.0.0 --port ${PORT:-8000}

function cleanup {
    echo "Shutting down development environment..."
    docker-compose -f docker-compose.local.yml down
    echo "Done!"
}

trap cleanup EXIT
