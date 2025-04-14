from app.modules.common.enum import StabilityType
from app.modules.stock_info.schemas import StabilityThreshold, StabilityTypeInfo

STABILITY_INFO = {
    StabilityType.FINANCIAL: StabilityTypeInfo(
        db_column="financial_stability_score",
        api_field="financial_data",
        description="재무 안정성 지표",
        threshold=StabilityThreshold(GOOD=0.8, BAD=0.2),
    ),
    StabilityType.PRICE: StabilityTypeInfo(
        db_column="price_stability_score",
        api_field="price_trend",
        description="가격 안정성 지표",
        threshold=StabilityThreshold(GOOD=0.8, BAD=0.2),
    ),
    StabilityType.MARKET: StabilityTypeInfo(
        db_column="market_stability_score",
        api_field="market_situation",
        description="시장 안정성 지표",
        threshold=StabilityThreshold(GOOD=0.7, BAD=0.3),
    ),
    StabilityType.SECTOR: StabilityTypeInfo(
        db_column="sector_stability_score",
        api_field="industry_situation",
        description="섹터 안정성 지표",
        threshold=StabilityThreshold(GOOD=0.8, BAD=0.2),
    ),
}
