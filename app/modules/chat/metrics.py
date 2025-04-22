from prometheus_client import Counter, Gauge, Histogram

# LLM 요청 관련 메트릭
LLM_REQUEST_COUNT = Counter("llm_requests_total", "Total number of LLM API requests", ["model", "status"])

LLM_REQUEST_DURATION = Histogram("llm_request_duration_seconds", "LLM API request duration in seconds", ["model"])

LLM_REQUEST_ERROR_COUNT = Counter(
    "llm_request_errors_total", "Total number of LLM API request errors", ["model", "error_type"]
)

# RabbitMQ 관련 메트릭
RABBITMQ_MESSAGES_PUBLISHED = Counter(
    "rabbitmq_messages_published_total", "Total number of messages published to RabbitMQ", ["queue"]
)

RABBITMQ_MESSAGES_CONSUMED = Counter(
    "rabbitmq_messages_consumed_total", "Total number of messages consumed from RabbitMQ", ["queue"]
)

RABBITMQ_PROCESSING_ERRORS = Counter(
    "rabbitmq_processing_errors_total", "Total number of errors during message processing", ["queue", "error_type"]
)

# 스트리밍 관련 메트릭
STREAMING_CONNECTIONS = Gauge("streaming_connections_current", "Current number of active streaming connections")

STREAMING_MESSAGES_COUNT = Counter(
    "streaming_messages_total", "Total number of streaming messages sent to clients", ["model", "conversation_id"]
)

STREAMING_ERRORS = Counter(
    "streaming_errors_total", "Total number of streaming errors", ["model", "error_type", "conversation_id"]
)
