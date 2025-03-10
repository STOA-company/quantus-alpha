#!/bin/bash
while true; do
  curl -s http://localhost/health-check | jq
  echo "Time: $(date)"
  sleep 1
done
