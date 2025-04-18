import pandas as pd

from app.batches.run_kr_etf_holdings import update_kr_etf_holdings
from app.core.logger.logger import setup_logger
from app.database.crud import database
from app.utils.krx import get_kr_etf_base_information

logger = setup_logger("fund_analysis", level="DEBUG")


def update_kr_etf_constituents(df: pd.DataFrame):
    # ticker 앞에 붙은 A를 제거
    df["ticker"] = df["ticker"].str.replace("^A", "", regex=True)
    # 구성 종목 수집을 위한 리스트 생성
    kr_etf_ticker_isin = df[["ticker", "isin"]].to_dict(orient="records")
    update_kr_etf_holdings(target_etf_list=kr_etf_ticker_isin)


def update_kr_ticker():
    df = get_kr_etf_base_information(is_download=True)

    need_columns = [
        "ISU_SRT_CD",  # ticker
        "ISU_CD",  # isin
        "ISU_ABBRV",  # kr_name
        "ISU_ENG_NM",  # en_name
    ]
    df = df[need_columns]

    rename_columns = {
        "ISU_SRT_CD": "ticker",
        "ISU_CD": "isin",
        "ISU_ABBRV": "kr_name",
        "ISU_ENG_NM": "en_name",
    }
    df.rename(columns=rename_columns, inplace=True)
    # ticker 앞에 A를 붙임
    df["ticker"] = "A" + df["ticker"]
    krx_tickers = df["ticker"].tolist()

    existing_tickers = database._select(
        table="stock_information",
        columns=["ticker"],
        ctry="kr",
        type="ETF",
        is_delisted=False,
    )
    existing_tickers = [row.ticker for row in existing_tickers]

    new_tickers = set(krx_tickers) - set(existing_tickers)

    new_ticker_df = df[df["ticker"].isin(new_tickers)]
    # 새로운 티커 구성종목 업데이트
    update_kr_etf_constituents(new_ticker_df)

    if new_ticker_df.empty:
        logger.info("No new tickers to update")
        return
    new_ticker_df["ctry"] = "kr"
    new_ticker_df["market"] = "KRX"
    new_ticker_df["is_activate"] = False
    new_ticker_df["is_pub"] = False
    new_ticker_df["type"] = "etf"

    database._insert(table="stock_information", sets=new_ticker_df.to_dict("records"))


if __name__ == "__main__":
    update_kr_ticker()
