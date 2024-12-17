from pydantic import BaseModel, Field


class StockInfo(BaseModel):
    homepage_url: str | None = Field(description="홈페이지 주소")
    ceo_name: str | None = Field(description="대표자 이름")
    establishment_date: str | None = Field(description="설립일")
    listing_date: str | None = Field(description="상장일")


class Indicators(BaseModel):
    per: float | None = Field(description="PER")
    pbr: float | None = Field(description="PBR")
    roe: float | None = Field(description="ROE")
