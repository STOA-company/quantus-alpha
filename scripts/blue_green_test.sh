#!/bin/bash
while true; do
  curl -s https://alpha-dev.quantus.kr/health-check | jq
  echo "Time: $(date)"
  sleep 1
done
