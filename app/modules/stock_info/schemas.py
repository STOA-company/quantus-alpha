from pydantic import BaseModel, Field


class StockInfo(BaseModel):
    homepage_url: str | None = Field(description="홈페이지 주소")
    ceo_name: str | None = Field(description="대표자 이름")
    establishment_date: str | None = Field(description="설립일")
    listing_date: str | None = Field(description="상장일")
