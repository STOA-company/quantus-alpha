#!/bin/bash

LOG_FILE="./monitoring/logs/prometheus_manager.log"
THRESHOLD=85  # 메모리 사용량 임계값 (%)

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> $LOG_FILE
}

restart_prometheus() {
  log "Restarting Prometheus container..."
  docker-compose -f monitoring/docker-compose.monitoring.yml restart prometheus
  log "Prometheus restarted."
}

clean_prometheus() {
  log "Performing deep cleaning of Prometheus..."

  docker-compose -f monitoring/docker-compose.monitoring.yml stop prometheus

  # 볼륨 백업
  # cp -r /var/lib/docker/volumes/quantus-alpha_prometheus_data /tmp/prometheus_backup_$(date +%Y%m%d)

  # 컨테이너 및 볼륨 제거
  docker-compose -f monitoring/docker-compose.monitoring.yml rm -f prometheus
  docker volume rm quantus-alpha_prometheus_data
  docker volume create quantus-alpha_prometheus_data

  # 컨테이너 재생성
  docker-compose -f monitoring/docker-compose.monitoring.yml up -d prometheus

  log "Prometheus deep cleaned and restarted."
}

check_prometheus() {
  if ! docker ps | grep -q "prometheus"; then
    log "Prometheus container not running. Starting..."
    docker-compose -f monitoring/docker-compose.monitoring.yml up -d prometheus
    return
  fi

  # 메모리 사용량 확인
  MEM_USAGE=$(docker stats prometheus --no-stream --format "{{.MemPerc}}" | sed 's/%//')

  if [ -z "$MEM_USAGE" ]; then
    log "Failed to get memory usage. Container might be in transition state."
    return
  fi

  log "Current memory usage: ${MEM_USAGE}%"

  if [ $(echo "$MEM_USAGE > $THRESHOLD" | bc) -eq 1 ]; then
    log "Memory usage exceeds threshold (${THRESHOLD}%). Taking action..."

    restart_prometheus

    sleep 300

    MEM_USAGE=$(docker stats prometheus --no-stream --format "{{.MemPerc}}" | sed 's/%//')

    if [ $(echo "$MEM_USAGE > $THRESHOLD" | bc) -eq 1 ]; then
      log "Memory still high (${MEM_USAGE}%) after restart. Performing deep clean..."
      clean_prometheus
    fi
  fi

  # WAL 세그먼트 수 확인
  WAL_SEGMENTS=$(docker exec prometheus ls -la /prometheus/wal 2>/dev/null | wc -l)
  log "Current WAL segments count: ${WAL_SEGMENTS}"

  # WAL 세그먼트가 과도하게 많으면 정리
  if [ "$WAL_SEGMENTS" -gt 1000 ]; then
    log "Excessive WAL segments detected (${WAL_SEGMENTS}). Performing deep clean..."
    clean_prometheus
  fi
}

# 스크립트 실행
check_prometheus
