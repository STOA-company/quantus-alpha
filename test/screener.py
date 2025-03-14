import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock

# 필요한 모듈 임포트
from app.modules.screener.stock.router import router  # 스크리너 라우터
from app.modules.screener.stock.service import ScreenerStockService
from app.utils.oauth_utils import get_current_user
from app.models.models_users import AlphafinderUser
from app.core.exception.custom import CustomException

# FastAPI 앱 대신 라우터를 직접 테스트
from fastapi import FastAPI

app = FastAPI()
app.include_router(router)  # 테스트할 라우터를 앱에 추가
client = TestClient(app)

# 테스트용 mock 데이터
MOCK_FACTORS = [
    {
        "factor": "PER",
        "description": "주가수익비율",
        "category": "valuation",
        "direction": "low",
        "min_value": 0,
        "max_value": 100,
        "unit": "%",
    },
    {
        "factor": "ROE",
        "description": "자기자본이익률",
        "category": "fundamental",
        "direction": "high",
        "min_value": -50,
        "max_value": 50,
        "unit": "%",
    },
]

MOCK_FILTERED_STOCKS = [
    {"Code": "AAPL", "Name": "Apple Inc.", "country": "US", "Market": "나스닥", "PER": {"value": 25.6, "unit": "%"}},
    {
        "Code": "MSFT",
        "Name": "Microsoft Corporation",
        "country": "US",
        "Market": "나스닥",
        "PER": {"value": 30.2, "unit": "%"},
    },
]

MOCK_GROUPS = [{"id": 1, "name": "기술주", "type": "stock"}, {"id": 2, "name": "배당주", "type": "stock"}]

MOCK_GROUP_FILTERS = {
    "name": "기술주",
    "stock_filters": [
        {"factor": "시장", "value": "US"},
        {"factor": "산업", "value": "Technology"},
        {"factor": "PER", "above": 10, "below": 30},
    ],
    "custom_factor_filters": ["PER", "ROE"],
    "has_custom": True,
}

MOCK_SECTORS = ["Technology", "Finance", "Healthcare", "Consumer Goods"]


# 사용자 인증 모킹
@pytest.fixture
def mock_current_user():
    user = MagicMock(spec=AlphafinderUser)
    user.id = 1
    user.username = "testuser"
    user.email = "test@example.com"
    return user


# 서비스 모킹
@pytest.fixture
def mock_screener_service():
    service = MagicMock(spec=ScreenerStockService)

    # 팩터 조회
    service.get_factors.return_value = MOCK_FACTORS

    # 필터링된 종목 조회
    service.get_filtered_data.return_value = (MOCK_FILTERED_STOCKS, len(MOCK_FILTERED_STOCKS))
    service.get_filtered_data_count.return_value = len(MOCK_FILTERED_STOCKS)

    # 그룹 관련
    service.get_groups.return_value = MOCK_GROUPS
    service.get_group_filters.return_value = MOCK_GROUP_FILTERS
    service.get_columns.return_value = ["PER", "ROE"]
    service.get_sort_info.return_value = {"sort_by": "PER", "ascending": False}
    service.get_available_sectors.return_value = MOCK_SECTORS

    # 그룹 관리
    service.create_group.return_value = True
    service.update_group.return_value = True
    service.delete_group.return_value = True
    service.reorder_groups.return_value = True
    service.update_group_name.return_value = "새 그룹명"

    return service


# 의존성 오버라이드 설정
@pytest.fixture
def override_dependencies(mock_current_user, mock_screener_service):
    app.dependency_overrides[get_current_user] = lambda: mock_current_user
    app.dependency_overrides[ScreenerStockService] = lambda: mock_screener_service
    yield
    app.dependency_overrides = {}


# 테스트 케이스


def test_get_factors(override_dependencies):
    """팩터 조회 테스트"""
    response = client.get("/factors/us")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == len(MOCK_FACTORS)
    assert data[0]["factor"] == MOCK_FACTORS[0]["factor"]
    assert data[0]["description"] == MOCK_FACTORS[0]["description"]
    assert data[0]["category"] == MOCK_FACTORS[0]["category"]


def test_get_factors_error(override_dependencies, mock_screener_service):
    """팩터 조회 예외 테스트"""
    # 서비스에서 예외 발생 시뮬레이션
    mock_screener_service.get_factors.side_effect = Exception("서비스 오류")

    response = client.get("/factors/us")
    assert response.status_code == 500
    assert "detail" in response.json()


def test_get_filtered_stocks(override_dependencies, mock_screener_service):
    """필터링된 종목 조회 테스트"""
    request_data = {
        "market_filter": "us",
        "sector_filter": ["Technology"],
        "custom_filters": [{"factor": "PER", "above": 10, "below": 30}],
        "factor_filters": ["PER", "ROE"],
        "limit": 10,
        "offset": 0,
        "sort_info": {"sort_by": "PER", "ascending": False},
        "lang": "kr",
    }

    response = client.post("/stocks", json=request_data)

    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "has_next" in data
    assert len(data["data"]) == len(MOCK_FILTERED_STOCKS)

    # 서비스 호출 확인
    mock_screener_service.get_filtered_data.assert_called_once()

    # 영어로 요청 테스트
    request_data["lang"] = "en"
    response = client.post("/stocks", json=request_data)
    assert response.status_code == 200


def test_get_filtered_stocks_custom_exception(override_dependencies, mock_screener_service):
    """필터링된 종목 조회 CustomException 테스트"""
    # CustomException 시뮬레이션
    mock_screener_service.get_filtered_data.side_effect = CustomException(message="커스텀 오류", status_code=400)

    request_data = {
        "market_filter": "us",
        "sector_filter": [],
        "custom_filters": [],
        "factor_filters": ["PER"],
        "limit": 10,
        "offset": 0,
        "lang": "kr",
    }

    response = client.post("/stocks", json=request_data)
    assert response.status_code == 400
    assert response.json()["detail"] == "커스텀 오류"


def test_get_filtered_stocks_general_exception(override_dependencies, mock_screener_service):
    """필터링된 종목 조회 일반 예외 테스트"""
    # 일반 예외 시뮬레이션
    mock_screener_service.get_filtered_data.side_effect = Exception("서비스 오류")

    request_data = {
        "market_filter": "us",
        "sector_filter": [],
        "custom_filters": [],
        "factor_filters": ["PER"],
        "limit": 10,
        "offset": 0,
        "lang": "kr",
    }

    response = client.post("/stocks", json=request_data)
    assert response.status_code == 500
    assert "detail" in response.json()


def test_get_filtered_stocks_count(override_dependencies, mock_screener_service):
    """필터링된 종목 개수 조회 테스트"""
    request_data = {
        "market_filter": "us",
        "sector_filter": ["Technology"],
        "custom_filters": [{"factor": "PER", "above": 10, "below": 30}],
        "factor_filters": ["PER"],
        "limit": 10,
        "offset": 0,
        "lang": "kr",
    }

    response = client.post("/stocks/count", json=request_data)

    assert response.status_code == 200
    data = response.json()
    assert "count" in data
    assert data["count"] == len(MOCK_FILTERED_STOCKS)

    # 서비스 호출 확인
    mock_screener_service.get_filtered_data_count.assert_called_once()


def test_get_groups(override_dependencies, mock_screener_service):
    """그룹 목록 조회 테스트"""
    response = client.get("/groups")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == len(MOCK_GROUPS)
    assert data[0]["id"] == MOCK_GROUPS[0]["id"]
    assert data[0]["name"] == MOCK_GROUPS[0]["name"]
    assert data[0]["type"] == MOCK_GROUPS[0]["type"]

    # 서비스 호출 확인
    mock_screener_service.get_groups.assert_called_once()


def test_get_groups_exception(override_dependencies, mock_screener_service):
    """그룹 목록 조회 예외 테스트"""
    # 예외 시뮬레이션
    mock_screener_service.get_groups.side_effect = Exception("서비스 오류")

    response = client.get("/groups")

    assert response.status_code == 200
    assert response.json() == []  # 예외 발생 시 빈 목록 반환


def test_create_group(override_dependencies, mock_screener_service):
    """그룹 생성 테스트"""
    request_data = {
        "name": "신규 그룹",
        "market_filter": "us",
        "sector_filter": ["Technology"],
        "custom_filters": [{"factor": "PER", "above": 10, "below": 30}],
        "type": "stock",
    }

    response = client.post("/groups", json=request_data)

    assert response.status_code == 200
    assert response.json()["message"] == "Group created successfully"

    # 서비스 호출 확인
    mock_screener_service.create_group.assert_called_once()


def test_update_group(override_dependencies, mock_screener_service):
    """그룹 업데이트 테스트"""
    request_data = {
        "id": 1,
        "name": "업데이트 그룹",
        "market_filter": "us",
        "sector_filter": ["Technology"],
        "custom_filters": [{"factor": "PER", "above": 10, "below": 30}],
        "factor_filters": ["PER", "ROE"],
        "type": "stock",
        "category": "valuation",
        "sort_info": {"sort_by": "PER", "ascending": False},
    }

    response = client.post("/groups", json=request_data)

    assert response.status_code == 200
    assert response.json()["message"] == "Filter updated successfully"

    # 서비스 호출 확인
    mock_screener_service.update_group.assert_called_once()


def test_create_group_custom_exception(override_dependencies, mock_screener_service):
    """그룹 생성 CustomException 테스트"""
    # CustomException 시뮬레이션
    mock_screener_service.create_group.side_effect = CustomException(
        message="이미 존재하는 그룹명입니다", status_code=400
    )

    request_data = {
        "name": "중복 그룹",
        "market_filter": "us",
        "sector_filter": [],
        "custom_filters": [],
        "factor_filters": ["PER"],
        "type": "stock",
    }

    response = client.post("/groups", json=request_data)

    assert response.status_code == 400
    assert response.json()["detail"] == "이미 존재하는 그룹명입니다"


def test_get_group_filters_default(override_dependencies, mock_screener_service):
    """기본 그룹 필터 조회 테스트"""
    response = client.get("/groups/-1")

    assert response.status_code == 200
    data = response.json()
    assert data["id"] == -1
    assert data["name"] == "기본"
    assert data["market_filter"] == "US"
    assert "factor_filters" in data
    assert "technical" in data["factor_filters"]
    assert "fundamental" in data["factor_filters"]
    assert "valuation" in data["factor_filters"]

    # 서비스 호출 확인
    mock_screener_service.get_available_sectors.assert_called_once()
    mock_screener_service.get_columns.assert_called()
    mock_screener_service.get_sort_info.assert_called()


def test_get_group_filters(override_dependencies, mock_screener_service):
    """특정 그룹 필터 조회 테스트"""
    response = client.get("/groups/1")

    assert response.status_code == 200
    data = response.json()
    assert data["name"] == MOCK_GROUP_FILTERS["name"]
    assert "factor_filters" in data
    assert "custom_filters" in data
    assert data["has_custom"]

    # 서비스 호출 확인
    mock_screener_service.get_group_filters.assert_called_once_with(1)
    mock_screener_service.get_columns.assert_called()
    mock_screener_service.get_sort_info.assert_called()


def test_get_group_filters_lang_en(override_dependencies, mock_screener_service):
    """영어 버전 그룹 필터 조회 테스트"""
    response = client.get("/groups/1?lang=en")

    assert response.status_code == 200
    data = response.json()
    assert data["name"] == MOCK_GROUP_FILTERS["name"]

    # 서비스 호출 확인
    mock_screener_service.get_group_filters.assert_called_once_with(1)


def test_delete_group(override_dependencies, mock_screener_service):
    """그룹 삭제 테스트"""
    response = client.delete("/groups/1")

    assert response.status_code == 200
    assert response.json()["message"] == "Group deleted successfully"

    # 서비스 호출 확인
    mock_screener_service.delete_group.assert_called_once_with(1)


def test_delete_group_failure(override_dependencies, mock_screener_service):
    """그룹 삭제 실패 테스트"""
    # 삭제 실패 시뮬레이션
    mock_screener_service.delete_group.return_value = False

    response = client.delete("/groups/1")

    assert response.status_code == 500
    assert response.json()["detail"] == "Failed to delete filter"


def test_reorder_groups(override_dependencies, mock_screener_service):
    """그룹 순서 변경 테스트"""
    response = client.post("/groups/reorder", json=[2, 1])

    assert response.status_code == 200
    assert response.json()["message"] == "Group reordered successfully"

    # 서비스 호출 확인
    mock_screener_service.reorder_groups.assert_called_once_with([2, 1])


def test_reorder_groups_failure(override_dependencies, mock_screener_service):
    """그룹 순서 변경 실패 테스트"""
    # 순서 변경 실패 시뮬레이션
    mock_screener_service.reorder_groups.return_value = False

    response = client.post("/groups/reorder", json=[2, 1])

    assert response.status_code == 500
    assert response.json()["detail"] == "Failed to reorder groups"


def test_update_group_name(override_dependencies, mock_screener_service):
    """그룹 이름 변경 테스트"""
    response = client.post("/groups/name?group_id=1&name=새이름")

    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert "새 그룹명" in data["message"]

    # 서비스 호출 확인
    mock_screener_service.update_group_name.assert_called_once_with(1, "새이름")


def test_update_group_name_custom_exception(override_dependencies, mock_screener_service):
    """그룹 이름 변경 CustomException 테스트"""
    # CustomException 시뮬레이션
    mock_screener_service.update_group_name.side_effect = CustomException(
        message="이미 존재하는 그룹명입니다", status_code=400
    )

    response = client.post("/groups/name?group_id=1&name=중복이름")

    assert response.status_code == 400
    assert response.json()["detail"] == "이미 존재하는 그룹명입니다"
