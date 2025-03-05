LOG_FILE="/home/ubuntu/parquet_update_log.txt"

echo "=== 작업 실행 $(date) ===" >> $LOG_FILE

response=$(curl -s -X GET https://alpha-dev.quantus.kr/api/v1/screener/parquet)

echo "결과: $response" >> $LOG_FILE
echo "" >> $LOG_FILE

if [[ $response == *"success"* ]]; then
  exit 0
else
  exit 1
fi
