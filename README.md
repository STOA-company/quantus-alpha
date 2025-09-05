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

1. PR(Pull Request)이 `staging` 브랜치로 머지될 때 (Staging 환경)
2. PR(Pull Request)이 `dev` 브랜치로 머지될 때 (Development 환경)

### 배포 워크플로우

1. 변경사항을 위한 새 브랜치 생성
2. 작업 완료 후 dev 또는 staging 브랜치로 PR 생성
3. 코드 리뷰 진행
4. PR 승인 및 머지
5. 자동 배포 워크플로우 실행

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




data: {"status": "submitted", "content": "주요 뉴스, 공시, 기업 이슈 등을 종합 분석하여 질문에 대한 답변을 준비하고 있습니다.", "job_id": "4377596f-8f6f-4aae-bc18-3bfcef4d1cda"}
data: {"status": "progress", "title": "research initiated", "content": "research initiated"}

data: {"status": "progress", "content": ":headphones: Listener is listening the query from request: 엔비디아 전망에 대해 알려줘"}

data: {"status": "progress", "content": "generating the research plans"}

data: {"status": "progress", "content": ":mag: Processing 5 sub-questions:\n1. 최근 엔비디아 관련 뉴스와 공개자료는 무엇이 있으며, 시장에 어떤 영향을 미치고 있나?\n2. 엔비디아와 관련된 최근 시장 동향과 업계 전망은 어떠한가?\n3. 엔비디아의 재무 상태와 주가 추세는 어떠하며, 앞으로의 성장 가능성은?\n4. 엔비디아와 경쟁사 또는 관련 기업의 동향은 어떠하며, 경쟁력 분석은 어떻게 이루어지고 있나?\n5. 엔비디아의 기술적 혁신과 장기 성장 전략은 무엇인가?"}

data: {"status": "progress", "content": ":bulb: Intermediate thought : ### 1. Research Plan\n\n**a) Stock Information and Financials Analysis**\n- Tool: `functions.get_stock_info`\n- Query: \"엔비디아 재무제표\" (Nvidia financial statements)\n- Parameters: limit 10, source type: all\n- Rationale: To gather comprehensive data on Nvidia's current financial health, including key financial ratios, assets, liabilities, and profitability indicators.\n\n**b) Stock Price Trend Analysis**\n- Tool: `functions.get_stock_price`\n- Query: \"엔비디아 주가 추이\" (Nvidia stock trend)\n- Parameters: period 1 ye..."}

data: {"status": "progress", "content": ":bulb: Intermediate thought : ### 1. Research Plan\n\n**Search 1: Recent Market Data for Nvidia**\n- Tool: functions.get_recent_data\n- Query: \"Nvidia\" recent market trends and industry outlook\n- Parameters: data_type=\"all\", ticker=\"NVDA\", days=30, limit=30\n- Rationale: To gather up-to-date quantitative data and recent news summaries about Nvidia's market performance and industry position.\n\n**Search 2: News and Industry Reports in Korean and US Markets**\n- Tool: functions.text_search\n- Query: \"엔비디아 최근 시장 동향과 업계 전망\"\n- Parameters:..."}

data: {"status": "progress", "content": ":bulb: Intermediate thought : ## 1. Research Plan\n\n### Search 1: Industry-wide analysis\n- Tool: `functions.text_search`\n- Query: \"Nvidia 경쟁사 시장 점유율, 기술력, 산업 동향\"\n- Parameters: limit 30, no specific source type\n- Rationale: This broad search aims to gather diverse perspectives on Nvidia's competitors, market share, technological advances, and industry trends to understand the competitive landscape.\n\n### Search 2: Recent developments and news\n- Tool: `functions.get_recent_data`\n- Query: Data type \"all\"\n- Parameters: limit 30 da..."}

data: {"status": "progress", "content": ":bulb: Intermediate thought : ### 1. Research Plan\n\n#### Search 1: Recent News and Disclosures on Nvidia and Market Impact\n- Tool: functions.semantic_vector_search\n- Query: \"Nvidia recent news and disclosures, market impact, latest developments\"\n- Parameters: src_types=[\"kor_news\",\"usa_news\",\"kor_disclosure\",\"usa_disclosure\"], limit=50\n- Rationale: To gather a broad overview of the latest news, disclosures, and market impact related to Nvidia from both Korean and US sources.\n\n#### Search 2: Future Outlook and Market Trends f..."}

data: {"status": "progress", "content": ":mag: Found 29 references from financial data sets"}

data: {"status": "progress", "content": "reviewing the research result [CYCLE : 1]"}

data: {"status": "progress", "content": ":memo: Re-planning based on feedback for cycle 1 | Issue: Lack of specific recent data, news articles, or em..."}

data: {"status": "progress", "content": ":mag: Processing 5 sub-questions:\n1. 최근 엔비디아 관련 뉴스와 공개자료는 무엇이 있으며, 시장에 어떤 영향을 미치고 있나?\n2. 엔비디아와 관련된 최근 시장 동향과 업계 전망은 어떠한가?\n3. 엔비디아의 재무 상태와 주가 추세는 어떠하며, 앞으로의 성장 가능성은?\n4. 엔비디아와 경쟁사 또는 관련 기업의 동향은 어떠하며, 경쟁력 분석은 어떻게 이루어지고 있나?\n5. 엔비디아의 기술적 혁신과 장기 성장 전략은 무엇인가?"}

data: {"status": "progress", "content": ":bulb: Intermediate thought : ### 1. Research Plan\n\n#### Search 1: Recent Market and Industry Trends\n- Tool: `functions.get_recent_data`\n- Query: Retrieve recent data on all types of news and disclosures to understand the current market environment and industry trends affecting Nvidia and its competitors.\n- Parameters: data_type = \"all\", limit = 30, timeframe = last 30 days\n- Rationale: To gather recent insights and market sentiment that could influence Nvidia's competitive positioning.\n\n#### Search 2: Nvidia's Corporate Pro..."}

data: {"status": "progress", "content": ":bulb: Intermediate thought : ### 1. Research Plan\n\n**Search 1: Nvidia의 기업 개요와 재무 정보 파악**\n- 도구: `functions.get_stock_info`\n- 검색어: \"Nvidia financials\"\n- 파라미터 구성: `company_name`에 \"Nvidia\", `limit` 5\n- 목적: Nvidia의 기본 기업 개요와 주요 재무 정보를 파악하여 재무 건전성의 기초 데이터 확보\n\n**검색 2: Nvidia의 최근 주가 추세 및 차트 분석**\n- 도구: `functions.get_stock_price`\n- 검색어: \"NVDA\"\n- 파라미터 구성: `ticker` \"NVDA\", `period` \"1y\", `interval` \"1d\"\n- 목적: 1년간 일별 종가 데이터를 통해 주가 추세와 변동성 분석\n\n**검색 3: Nvidia의 최신 재무제표와 성장 가능성 관련 뉴스/공시 자료 수집**\n- 도구: `functions.get_recent_data`\n- 검색어: \"Nvi..."}

data: {"status": "progress", "content": ":bulb: Intermediate thought : ### 1. Research Plan\n\n**Search 1: Broad exploration of Nvidia's technological innovation and long-term growth strategy**\n- Tool: functions.semantic_vector_search\n- Query: \"Nvidia 기술 혁신 동향과 장기 성장 전략\"\n- Parameters: src_types=[\"kor_news\", \"usa_news\", \"kor_disclosure\", \"usa_disclosure\"], limit=30\n- Rationale: To gather diverse perspectives and recent developments on Nvidia's technological advancements and strategic vision for sustained growth.\n\n**Search 2: Focused analysis of Nvidia's future growth ..."}

data: {"status": "progress", "content": ":mag: Found 15 references from financial data sets"}

data: {"status": "progress", "content": "reviewing the research result [CYCLE : 2]"}

data: {"status": "progress", "content": ":memo: Re-planning based on feedback for cycle 2 | Issue: Some statements lack direct citation or specific d..."}

data: {"status": "progress", "content": ":mag: Processing 5 sub-questions:\n1. 최근 엔비디아 관련 뉴스와 공개자료는 무엇이 있으며, 시장에 어떤 영향을 미치고 있나?\n2. 엔비디아와 관련된 최근 시장 동향과 업계 전망은 어떠한가?\n3. 엔비디아의 재무 상태와 주가 추세는 어떠하며, 앞으로의 성장 가능성은?\n4. 엔비디아와 경쟁사 또는 관련 기업의 동향은 어떠하며, 경쟁력 분석은 어떻게 이루어지고 있나?\n5. 엔비디아의 기술적 혁신과 장기 성장 전략은 무엇인가?"}

data: {"status": "progress", "content": ":bulb: Intermediate thought : Based on the recent data, Nvidia remains a highly active subject in the market with substantial positive market impact signals, indicating strong market interest and possibly favorable sentiment. The volume of disclosures and news suggests ongoing competitive developments, technological advancements, and strategic moves by Nvidia and its competitors. While specific company data and sector ETF analyses are pending, the overall trend points toward Nvidia maintaining a robust competitive position, ..."}

data: {"status": "progress", "content": ":bulb: Intermediate thought : ## Research Plan\n\n1. **Market Data and Recent Developments Search**\n   - Tool: `functions.get_recent_data`\n   - Query: \"Nvidia\"\n   - Parameters: data_type=\"all\", days=30, limit=30\n   - Rationale: To gather recent news, disclosures, and market movements related to Nvidia for the past month, providing a snapshot of current market dynamics and company-specific developments.\n\n2. **Comprehensive Industry Trend and Outlook Search**\n   - Tool: `functions.text_search`\n   - Query: \"엔비디아 최근 시장 동향과 업계 전망\"\n..."}

data: {"status": "progress", "content": ":mag: Found 6 references from financial data sets"}

data: {"status": "progress", "content": "reviewing the research result [CYCLE : 3]"}

data: {"status": "progress", "content": "started to generate the final report"}

data: {"status": "success", "content": "# 엔비디아 미래 전망 종합 보고서\n\n## 핵심 요약\n- 엔비디아는 인공지능, 데이터 센터, 자율주행, 메타버스 등 핵심 산업에서 지속적인 기술 혁신과 전략적 확장을 추진하며 시장 기대를 높이고 있음.\n- 최근 뉴스와 공개자료는 엔비디아의 글로벌 협력 강화, 차세대 GPU 개발, AI 생태계 확장에 집중하는 전략을 보여줌.\n- 재무적으로도 1년간 주가가 약 46% 상승하며 강력한 성장 모멘텀을 유지하고 있으며, 수익성도 높게 기록되고 있음.\n- 시장 동향은 글로벌 수요 증가와 AI/반도체 시장의 성장에 힘입어 장기 성장 잠재력이 매우 높음.\n- 경쟁사 대비 기술적 우위와 시장 점유력을 확보하며, 글로벌 협력 및 신사업 다변화 전략으로 지속 성장이 기대됨.\n- 다만, 글로벌 공급망 문제와 지정학적 리스크는 잠재적 리스크로 남아 있으나, 현재 자료들은 엔비디아의 장기 성장 가능성을 높게 평가하는 방향임.\n\n## 상세 분석\n\n### 1. 시장 및 업계 동향\n- 글로벌 AI 및 반도체 시장은 연평균 성장률이 높으며, 엔비디아는 AI 하드웨어와 데이터 센터 시장에서 선도적 위치를 유지하고 있음.\n- 최근 수집한 뉴스와 공개자료에 따르면, 엔비디아는 차세대 GPU 개발, 글로벌 협력 확대, 친환경 기술 도입 등에 적극 투자하고 있으며, 인공지능 생태계 확장 전략을 추진 중.\n- 글로벌 협력 사례로는 한국, 미국, 유럽 등에서 표준화와 기술 개발 협력 강화를 보여주고 있음.\n\n### 2. 재무 상태와 주가 추세\n- 최근 1년간 주가가 약 46% 상승하며 시장의 신뢰를 받고 있으며, 매출과 영업이익 모두 역대 최고치를 기록.\n- 실적 발표에 따르면, 데이터 센터와 AI 관련 사업이 수익성 향상에 크게 기여.\n- 글로벌 시장에서 AI와 반도체 수요 증가에 힘입어, 장기 성장 기대감이 크며, 재무 안정성도 확보된 상태임.\n\n### 3. 기술 혁신 및 전략\n- 최신 GPU 아키텍처와 AI 칩 개발에 박차를 가하며, 자율주행과 메타버스 등 신사업 분야에 집중.\n- 친환경 기술과 차세대 칩 설계로 경쟁우위 확보, 글로벌 파트너십 확대, 인수합병 전략으로 시장 지배력 강화.\n- 연구개발 투자도 지속적으로 확대하며, AI 초거대 모델과 차세대 클라우드 인프라 강화를 목표로 함.\n\n### 4. 시장 전망 및 경쟁력\n- 인공지능 및 반도체 시장의 지속적 성장 기대 속에서, 엔비디아는 기술적 우위와 시장 점유율 확대를 통해 장기 성장 모멘텀 유지.\n- 경쟁사 대비 기술력, 시장 지배력, 글로벌 협력 네트워크 강화를 통해 글로벌 시장 내 입지 강화.\n- 시장 전문가들은 엔비디아의 미래 성장 잠재력을 매우 높게 평가하며, 장기적 투자 가치가 크다고 보고 있음.\n\n### 5. 리스크 및 한계\n- 글로벌 공급망 불안과 지정학적 리스크는 잠재적 장애요인.\n- 일부 최신 뉴스와 경쟁사 동향 분석이 아직 부족한 점은 보완이 필요.\n- 환율 변동, 규제 강화 등 외부 변수도 고려해야 함.\n\n## 결론 및 추천\n엔비디아는 기술 혁신, 재무 안정성, 글로벌 협력 강화 등 다양한 강점을 바탕으로 향후 수년간 지속 성장 가능성이 매우 높음. AI와 반도체 시장의 성장세에 힘입어, 장기적 투자 관점에서 매우 유망한 기업으로 평가됨. 다만, 글로벌 공급망 문제와 경쟁 심화에 대비한 리스크 관리가 필요하며, 시장 동향을 지속 모니터링하는 전략이 중요함.\n\n---\n\n이상으로 엔비디아의 미래 전망에 대한 데이터 기반 종합 보고를 마칩니다."}↵↵