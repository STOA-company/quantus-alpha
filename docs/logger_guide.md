# Logger 모듈 사용 가이드

이 문서는 애플리케이션의 로깅 모듈 사용 방법을 설명합니다. 효율적인 로깅을 통해 애플리케이션 동작을 모니터링하고 문제를 신속하게 파악할 수 있습니다.

## 목차

1. [개요](#개요)
2. [기본 사용법](#기본-사용법)
3. [로그 레벨](#로그-레벨)
4. [로그 포맷](#로그-포맷)
5. [파일 로깅](#파일-로깅)
6. [Slack 알림 설정](#slack-알림-설정)
7. [날짜별 로그 관리](#날짜별-로그-관리)
8. [로그 회전](#로그-회전)
9. [고급 사용 예시](#고급-사용-예시)
10. [FAQ](#faq)

## 개요

Logger 모듈은 파이썬의 기본 로깅 시스템을 래핑하여 확장된 기능을 제공합니다:

- 콘솔 및 파일 로깅 동시 지원
- 날짜별 로그 디렉토리 자동 관리
- 에러 로그 자동 분리
- Slack 알림 통합
- 로그 파일 S3 자동 업로드 지원
- 로그 파일 자동 회전 및 보관

## 기본 사용법

### 로거 가져오기

```python
from app.core.logger import get_logger, setup_logger

# 방법 1: 기존 로거 가져오기
logger = get_logger("module_name")

# 방법 2: 새 로거 생성하기
logger = setup_logger("module_name", level="DEBUG")
```

### 로그 작성하기

```python
logger.debug("디버그 메시지")
logger.info("정보 메시지")
logger.warning("경고 메시지")
logger.error("에러 메시지")
logger.critical("치명적 오류 메시지")

# 예외 정보 포함
try:
    # 예외가 발생할 수 있는 코드
    raise ValueError("샘플 예외")
except Exception as e:
    logger.exception("예외가 발생했습니다")  # 자동으로 예외 정보와 스택 트레이스 출력
```

## 로그 레벨

로거는 다음 로그 레벨을 지원합니다(낮은 순서부터):

1. `DEBUG` - 디버깅 목적의 자세한 정보
2. `INFO` - 정상 작동 확인을 위한 정보
3. `WARNING` - 경고(잠재적 문제)
4. `ERROR` - 오류(기능 실패)
5. `CRITICAL` - 치명적 오류(프로그램 중단 가능)

기본 로그 레벨은 `INFO`이며, 로거 설정 시 변경할 수 있습니다:

```python
logger = setup_logger("module_name", level="DEBUG")  # 문자열 형태로 지정
# 또는
logger = setup_logger("module_name", level=logging.DEBUG)  # logging 모듈 상수 사용
```

## 로그 포맷

기본 로그 포맷은 다음과 같습니다:

```
%(asctime)s - %(levelname)s - [%(name)s] %(message)s
```

예시: `2025-04-10 15:32:45,123 - INFO - [app.module] 메시지 내용`

포맷을 변경하려면:

```python
logger = setup_logger("module_name", log_format="%(asctime)s | %(levelname)8s | %(message)s")
```

## 파일 로깅

모든 로그는 자동으로 파일에 저장됩니다. 기본 설정:

- 로그 디렉토리: `logs/`
- 로그 파일: `{로거이름}.log`
- 에러 로그 파일(ERROR 이상 레벨): `{로거이름}_error.log`

로그 디렉토리 변경:

```python
logger = setup_logger("module_name", log_dir="custom/log/path")
```

에러 로그 분리 비활성화:

```python
logger = setup_logger("module_name", separate_error_logs=False)
```

## Slack 알림 설정

에러 로그를 Slack으로 받아보려면:

```python
logger = setup_logger(
    "module_name",
    send_error_to_slack=True,
    slack_webhook_url="https://hooks.slack.com/services/YOUR_WEBHOOK_URL"
)

# 이제 ERROR 레벨 이상의 로그 메시지가 Slack으로 전송됩니다
logger.error("이 메시지는 Slack에도 표시됩니다")
```

Slack 설정 옵션:

```python
logger = setup_logger(
    "module_name",
    send_error_to_slack=True,
    slack_webhook_url="https://hooks.slack.com/services/YOUR_WEBHOOK_URL",
    slack_channel="default",  # webhook_urls 딕셔너리의 채널 키
    slack_username="Logger Bot",  # Slack에 표시될 봇 이름
    slack_icon_emoji=":warning:"  # 봇 아이콘 이모지
)
```

여러 채널에 알림 보내기:

```python
from app.core.logger.config import configure

# 전역 설정으로 여러 채널 등록
configure(
    slack_webhook_urls={
        "default": "https://hooks.slack.com/services/DEFAULT_URL",
        "critical": "https://hooks.slack.com/services/CRITICAL_URL",
        "payment": "https://hooks.slack.com/services/PAYMENT_URL"
    }
)

# 채널 선택
logger.error("결제 오류", extra={"slack_channel": "payment"})
```

## 날짜별 로그 관리

기본적으로 로그는 날짜별 폴더로 관리됩니다:

```
logs/
  ├── 2025-04-09/
  │   ├── module_name.log
  │   └── module_name_error.log
  └── 2025-04-10/
      ├── module_name.log
      └── module_name_error.log
```

날짜 폴더 관리 비활성화:

```python
logger = setup_logger("module_name", use_date_folders=False)
```

날짜 폴더 형식 변경:

```python
logger = setup_logger("module_name", date_folder_format="%Y%m%d")  # 예: 20250410
```

## 로그 회전

로그 파일이 너무 커지지 않도록 자동 회전을 설정할 수 있습니다:

```python
logger = setup_logger(
    "module_name",
    use_date_folders=False,  # 날짜별 폴더 사용 시 회전 필요 없음
    rotate_logs=True,
    rotation_interval="daily",  # daily, hourly, weekly, monthly
    backup_count=30  # 보관할 이전 로그 파일 수
)
```


## 커스텀 로거 설정

전역 설정을 사용하여 모든 로거의 기본 동작을 변경할 수 있습니다:

```python
from app.core.logger.config import configure

# 전역 기본 설정 변경
configure(
    log_level="INFO",
    log_dir="custom/logs",
    log_format="%(asctime)s - %(levelname)s - [%(name)s] %(message)s",
    separate_error_logs=True,
    use_date_folders=True,
    send_error_to_slack=True,
    slack_webhook_url="https://hooks.slack.com/services/YOUR_URL"
)

# 이후 생성된 모든 로거는 위 설정을 기본값으로 사용
logger1 = get_logger("module1")  # 위 설정 사용
logger2 = get_logger("module2")  # 위 설정 사용

# 개별 로거에서 설정 오버라이드 가능
logger3 = setup_logger("module3", level="DEBUG", separate_error_logs=False)
```
