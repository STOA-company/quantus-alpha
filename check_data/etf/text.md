etf factor 추출하기 위한 사전 데이터를 모아놓은 폴더입니다.

아래 파일들이 해당 폴더에 저장됩니다.
- {ctry}_etf_dividend.parquet: {ctry} ETF 배당 데이터
- {ctry}_etf_dividend_factor.parquet: {ctry} ETF 배당 팩터 데이터
- {ctry}_etf_price.parquet: {ctry} ETF 가격 데이터
- {ctry}_etf_price_factor.parquet: {ctry} ETF 가격 팩터 데이터
예시:
- kr_etf_dividend.parquet: 한국 ETF 배당 데이터
- kr_etf_price.parquet: 한국 ETF 가격 데이터
- us_etf_dividend.parquet: 미국 ETF 배당 데이터
- us_etf_price.parquet: 미국 ETF 가격 데이터

경로 변경 시 `app/common/constants.py` 의 `ETF_DATA_DIR` 변수도 함께 변경해야 합니다.
