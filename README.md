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

## 배포 자동화 (GitHub Actions)

GitHub Actions을 사용하여 자동 배포를 설정했습니다. 배포는 다음과 같은 조건에서 자동으로 실행됩니다:

1. `staging` 브랜치에 변경사항이 푸시될 때 (Staging 환경)
2. `dev` 브랜치에 변경사항이 푸시될 때 (Development 환경)

또한 GitHub UI에서 수동으로 워크플로우를 실행할 수 있습니다:

1. GitHub 저장소의 "Actions" 탭으로 이동
2. "Deploy Service" 워크플로우 선택
3. "Run workflow" 버튼 클릭
4. 배포 환경 선택 (dev, staging)
5. 캐시 정리 여부 선택 (필요한 경우)
6. "Run workflow" 클릭

### GitHub Actions 배포를 위한 설정

배포 자동화를 사용하려면 GitHub 저장소의 Secrets에 다음 값들을 설정해야 합니다:

1. 개발 환경(dev):
   - `DEV_SERVER_HOST`: 개발 서버 호스트명 또는 IP 주소
   - `DEV_SSH_USER`: 개발 서버 SSH 접속 계정명
   - `DEV_SSH_PRIVATE_KEY`: 개발 서버 접속용 SSH 개인키 (BEGIN부터 END까지 전체)

2. 스테이징 환경(staging):
   - `STAGING_SERVER_HOST`: 스테이징 서버 호스트명 또는 IP 주소
   - `STAGING_SSH_USER`: 스테이징 서버 SSH 접속 계정명
   - `STAGING_SSH_PRIVATE_KEY`: 스테이징 서버 접속용 SSH 개인키 (BEGIN부터 END까지 전체)

이 값들은 GitHub 저장소의 Settings > Secrets and variables > Actions 메뉴에서 설정할 수 있습니다.

#### Secrets 설정 방법:
1. GitHub 프로젝트 저장소로 이동
2. 상단 메뉴의 "Settings" 탭 클릭
3. 왼쪽 사이드바에서 "Secrets and variables" > "Actions" 선택
4. "New repository secret" 버튼을 클릭하여 각 Secret 추가
5. 각 Secret의 이름과 값을 정확히 입력

**SSH_PRIVATE_KEY 값 예시:**
```
-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAABlwAAAAdzc2gtcn
...
(키 내용 전체)
...
AAB3KbJmKTkaz/0hTJkOQJYTL9Ieaa8EMJMSBQiN/GJT1mA4nY=
-----END OPENSSH PRIVATE KEY-----
```

**주의**:
- SSH 키는 절대 공개되어서는 안 됩니다. 반드시 비공개 Secret으로 관리하세요.
- 각 서버에는 해당하는 SSH 공개키를 `~/.ssh/authorized_keys` 파일에 등록해야 합니다.
