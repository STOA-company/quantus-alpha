# Quantus Alpha

Quantus Alpha는 FastAPI를 기반으로 한 Python 웹 애플리케이션입니다.

## 로컬 환경 설치 및 실행 방법

### pyenv 및 Python 설치

```bash
brew install pyenv
pyenv install 3.11.0
```

### Poetry 설치

Poetry는 여러 가지 방법으로 설치할 수 있습니다. 아래는 몇 가지 일반적인 방법입니다:

1. pipx를 사용한 설치 (권장):

    ```bash
    brew install pipx
    pipx ensurepath
    pipx install poetry
    ```

2. pip을 사용한 설치:

    ```bash
    pip install poetry
    ```

3. 공식 설치 스크립트 사용:
    ```bash
    curl -sSL https://install.python-poetry.org | python3 -
    ```

설치 후, 환경 변수 경로를 추가해야 할 수 있습니다. 대부분의 경우 자동으로 처리되지만, 수동으로 추가해야 한다면:

```bash
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc  # 또는 ~/.zshrc
source ~/.bashrc  # 또는 source ~/.zshrc
```

### 프로젝트 설치

```bash
poetry install
```

## 개발 환경에서 실행하기

1. Poetry 가상 환경 활성화:

    ```bash
    poetry shell
    ```

2. Uvicorn을 사용하여 FastAPI 서버 실행:

    ```bash
    uvicorn app.main:app --reload
    ```

    - `app.main:app`은 `app` 디렉토리의 `main.py` 파일에서 `app` 객체를 찾아 실행한다는 의미입니다.
    - `--reload` 옵션은 코드 변경 시 서버를 자동으로 재시작합니다 (개발 시 유용).

3. 브라우저에서 `http://localhost:8000`으로 접속하여 API 확인

4. API 문서는 `http://localhost:8000/docs`에서 확인 가능합니다.

## 상용 서비스 실행 (Docker Compose)

### 환경 변수 설정

```bash
cp .env.example .env.prod
```

### 이미지 빌드

```bash
# 모든 이미지 빌드
docker-compose build

# (선택사항) 캐시 없이 빌드
docker-compose build --no-cache
```

### 서비스 실행

```bash
# 모든 서비스 실행
docker-compose up -d

# 특정 서비스 실행
docker-compose up -d {service_name}
```

### 서비스 재실행

서비스 재실행은 새로운 이미지를 사용하지 않습니다. 이미지를 새로 빌드한 경우 시스템을 중지한 후 다시 실행해야 합니다.

```bash
# 전체 서비스 재실행
docker-compose restart

# web 서비스 재실행
docker-compose restart web nginx
```

### 서비스 중지

```bash
# 전체 서비스 중지
docker-compose down

# web, nginx 서비스 중지
docker-compose down web nginx
```

### 서비스 로그 확인

```bash
docker-compose logs -f --tail=100 {service_name}
```

## 주요 의존성

-   fastapi: ^0.115.2
-   uvicorn: ^0.32.0
-   pandas: ^2.2.3
-   pyarrow: ^17.0.0
-   pydantic-settings: ^2.5.2
-   httpx: ^0.27.2
-   boto3: ^1.35.42
-   jupyter: ^1.1.1
-   ipykernel: ^6.29.5

## 문제 해결

1. **포트 충돌**: 80번 포트가 이미 사용 중이라면 `docker-compose.yml` 파일에서 포트 매핑을 변경하세요.

2. **빌드 실패**: 빌드 중 오류가 발생하면 로그를 확인하고 필요한 종속성이 모두 `pyproject.toml`에 명시되어 있는지 확인하세요.

3. **컨테이너 재시작 문제**: 컨테이너가 계속 재시작되면 애플리케이션 로그를 확인하여 오류를 찾으세요.

    ```
    docker-compose logs web
    ```

4. **볼륨 마운트**: 개발 중 코드 변경사항을 바로 반영하려면 `docker-compose.yml`에 볼륨이 올바르게 설정되어 있는지 확인하세요.

## 환경 변수

`docker-compose.yml`에서 다음과 같이 환경 변수를 설정할 수 있습니다:

-   `ENV`: 기본값은 `prod`입니다. 개발 환경에서는 `dev`로 설정할 수 있습니다.

예시:

```yaml
services:
    web:
        environment:
            - ENV=prod
```

`.env.prod` 파일을 사용하여 프로덕션 환경 변수를 관리할 수 있습니다. 필요에 따라 `.env.dev` 파일을 생성하여 개발 환경 변수를 별도로 관리할 수 있습니다.
