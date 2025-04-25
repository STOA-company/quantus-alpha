import datetime
from typing import List, Set, Tuple

import pandas as pd
from sqlalchemy import text

from app.core.logger.logger import setup_logger
from app.database.crud import database
from app.utils.krx import get_all_prices, get_etf_holdings

logger = setup_logger("kr_etf_holdings", level="DEBUG")


def get_kr_etf_set() -> Set[Tuple[str, str]]:
    """
    Get a list of Korean ETFs from KRX.
    Returns:
        Set[Tuple[str, str]]: Set of (ticker, isin) tuples
    """
    df = get_all_prices(asset="etf")
    need_columns = ["ISU_SRT_CD", "ISU_CD"]
    df = df[need_columns]
    df = df.rename(columns={"ISU_SRT_CD": "ticker", "ISU_CD": "isin"})

    # Convert DataFrame to set of tuples
    etf_set = set(zip(df["ticker"], df["isin"]))
    return etf_set


def calculate_weight(row: pd.Series, cash_amount: str | None) -> float | None:
    """
    Calculate weight based on the given row and cash amount.

    Args:
        row: DataFrame row containing holding information
        cash_amount: Amount from CASH00000001 holding

    Returns:
        float | None: Calculated weight or None if cannot be calculated
    """
    if row["weight"] != "-":
        return float(row["weight"])

    if not cash_amount or cash_amount == "-":
        return None

    denominator = float(cash_amount.replace(",", ""))
    if row["amount"] != "-":
        numerator = float(row["amount"].replace(",", ""))
        return (numerator / denominator) * 100

    return None


def format_holding_ticker(holding_ticker: str, isin: str) -> str:
    """
    Format holding ticker according to the rules:
    - If holding_ticker is not an ISIN (i.e., it's a valid ticker), add 'A' prefix
    - If holding_ticker is an ISIN, keep it as is

    Args:
        holding_ticker (str): The holding ticker to format
        isin (str): The ISIN code for comparison

    Returns:
        str: Formatted holding ticker
    """
    if holding_ticker != isin:
        return "A" + holding_ticker
    return holding_ticker


def get_kr_etf_holdings(isin_list: List[str], ticker_list: List[str]) -> List[Tuple[str, float]]:
    """
    Get holdings information for a Korean ETF using KIS API.
    Args:
        isin_list (List[str]): The ETF isin list
    Returns:
        List[Tuple[str, float]]: List of (holding_ticker, weight) tuples
    """
    holdings_list = []
    for isin, ticker in zip(isin_list, ticker_list):
        df = get_etf_holdings(isin)
        need_columns = [
            "COMPST_ISU_CD",  # ticker
            "COMPST_ISU_CD2",  # isin
            "COMPST_ISU_CU1_SHRS",  # shares
            "COMPST_ISU_NM",  # name
            "COMPST_RTO",  # weight
            "COMPST_AMT",  # amount
            "VALU_AMT",  # valuation amount
        ]
        df = df[need_columns]
        df = df.rename(
            columns={
                "COMPST_ISU_CD": "holding_ticker",
                "COMPST_ISU_CD2": "isin",
                "COMPST_ISU_CU1_SHRS": "shares",
                "COMPST_ISU_NM": "name",
                "COMPST_RTO": "weight",
                "COMPST_AMT": "amount",
                "VALU_AMT": "valuation_amount",
            }
        )
        logger.debug(f"rename_df: \n {df}")

        # CASH00000001의 amount를 분모로 사용
        cash_amount = (
            df[df["holding_ticker"] == "CASH00000001"]["amount"].iloc[0]
            if not df[df["holding_ticker"] == "CASH00000001"].empty
            else None
        )

        # CASH00000001과 KRD010010001 제외
        exclude_list = ["CASH00000001", "KRD010010001"]
        df = df[~df["holding_ticker"].isin(exclude_list)]

        # weight 계산
        for _, row in df.iterrows():
            weight = calculate_weight(row, cash_amount)

            formatted_holding_ticker = format_holding_ticker(row["holding_ticker"], row["isin"])

            holdings_list.append(
                {
                    "ticker": ticker,
                    "holding_ticker": formatted_holding_ticker,
                    "isin": row["isin"],
                    "shares": None if row["shares"] == "-" else row["shares"],
                    "name": row["name"],
                    "weight": weight,
                    "updated_at": datetime.datetime.now(),
                }
            )
        logger.debug(f"holdings_list: \n {holdings_list}")
    return holdings_list


def get_existing_holdings(target_ticker_list: List[str]) -> Set[Tuple[str, str, float]]:
    """
    Get existing holdings from the database.
    Args:
        etf_list: List of ETF symbols to check
    Returns:
        Set of (ticker, isin, weight) tuples # TODO: 추후 isin 수정해야함.
    """
    existing_holdings = database._select(
        table="etf_top_holdings", columns=["ticker", "isin", "weight"], ticker__in=target_ticker_list
    )
    existing_holdings = {(row.ticker, row.isin, row.weight) for row in existing_holdings}
    return existing_holdings


def delete_old_holdings(holdings_to_delete: List[Tuple[str, str, float]]) -> None:
    """
    Delete holdings that no longer exist in the new data.

    Args:
        holdings_to_delete: List of (ticker, isin, weight) tuples to delete
    """
    if not holdings_to_delete:
        logger.info("No holdings to delete")
        return

    logger.info(f"Preparing to delete {len(holdings_to_delete)} old holdings")
    delete_conditions = []
    for ticker, isin, _ in holdings_to_delete:
        delete_conditions.append(f"(ticker = '{ticker}' AND isin = '{isin}')")

    delete_query = f"""
        DELETE FROM etf_top_holdings
        WHERE {' OR '.join(delete_conditions)}
    """
    try:
        database._execute(text(delete_query))
        logger.info(f"Successfully deleted {len(holdings_to_delete)} holdings that no longer exist")
    except Exception as e:
        logger.error(f"Error deleting old holdings: {str(e)}")
        raise


def insert_new_holdings(holdings_to_add: List[dict]) -> None:
    """
    Insert new holdings into the database.

    Args:
        holdings_to_add: List of holding dictionaries to insert
    """
    if not holdings_to_add:
        logger.info("No holdings to add")
        return

    logger.info(f"Preparing to insert {len(holdings_to_add)} new holdings")
    try:
        database._insert(table="etf_top_holdings", sets=holdings_to_add)
        logger.info(f"Successfully inserted {len(holdings_to_add)} new holdings")
    except Exception as e:
        logger.error(f"Error inserting new holdings: {str(e)}")
        raise


def update_holdings_weights(holdings_to_update: List[dict]) -> None:
    """
    Update the weights of holdings in the database.

    Args:
        holdings_to_update: List of holding dictionaries to update
    """
    if not holdings_to_update:
        logger.info("No holdings to update")
        return

    logger.info(f"Preparing to update {len(holdings_to_update)} holdings")
    try:
        # 복합 키(ticker, isin)를 사용하여 bulk update
        database._bulk_update_multi_key(
            table="etf_top_holdings",
            data=holdings_to_update,
            key_columns=["ticker", "isin"],
            chunk_size=50,  # 적절한 청크 크기 설정
        )
        logger.info(f"Successfully updated {len(holdings_to_update)} holdings")
    except Exception as e:
        logger.error(f"Error updating holdings: {str(e)}")
        raise


def update_kr_etf_holdings(
    chunk_size: int = 10, weight_threshold: float = 0.01, target_etf_list: List[Tuple[str, str]] = None
):
    """
    Update the holdings for all Korean ETFs in the database.
    Workflow:
    1. Get list of Korean ETFs from database
    2. Process ETFs in chunks:
       2-1. Get existing holdings from database
       2-2. Fetch new holdings from KIS API
       2-3. Compare and classify data:
           - Delete group: holdings that no longer exist
           - Add group: new holdings
           - Update group: holdings with changed weights
       2-4. Process each group:
           - Delete removed holdings
           - Add new holdings
           - Update changed weights
    Args:
        chunk_size: Number of ETFs to process in each chunk
        weight_threshold: Minimum weight difference to trigger an update
        target_etf_list: List of (ticker, isin) tuples to process
    """
    # 1. 종목 리스트 가져오기
    if target_etf_list is None:
        etf_set = get_kr_etf_set()
    else:
        # Convert list of dictionaries to set of tuples
        etf_set = {(item["ticker"], item["isin"]) for item in target_etf_list}
    target_isin_list = [item[1] for item in etf_set]
    target_ticker_list = ["A" + item[0] for item in etf_set]

    # 2. 청크 나누기
    for i in range(0, len(etf_set), chunk_size):
        chunk_isin_list = target_isin_list[i : i + chunk_size]
        chunk_ticker_list = target_ticker_list[i : i + chunk_size]
        # 2-1. 기존 홀딩 가져오기
        existing_holdings = get_existing_holdings(chunk_ticker_list)
        # 2-2. 새로운 홀딩 가져오기
        new_holdings = get_kr_etf_holdings(chunk_isin_list, chunk_ticker_list)
        # 2-3. 비교 및 분류
        # 2-3-1. 삭제할 홀딩 찾기
        existing_keys = {h[:2] for h in existing_holdings}
        new_holdings_keys = {(h["ticker"], h["isin"]) for h in new_holdings}
        holdings_to_delete = existing_keys - new_holdings_keys
        holdings_to_delete = [h for h in existing_holdings if (h[0], h[1]) in holdings_to_delete]
        # 2-3-2. 추가할 홀딩 찾기
        holdings_to_add = new_holdings_keys - existing_keys
        holdings_to_add = [h for h in new_holdings if (h["ticker"], h["isin"]) in holdings_to_add]
        # 2-3-3. 변경된 홀딩 찾기
        holdings_to_update = []
        holdings_to_update_keys = existing_keys & new_holdings_keys
        for ticker, holding, old_weight in existing_holdings:
            if (ticker, holding) in holdings_to_update_keys:
                # Find the matching holding in new_holdings
                matching_holding = next((h for h in new_holdings if h["ticker"] == ticker and h["isin"] == holding), None)
                if matching_holding:
                    new_weight = matching_holding["weight"]
                    if (
                        (new_weight is None and old_weight is not None)
                        or (new_weight is not None and old_weight is None)
                        or (
                            new_weight is not None
                            and old_weight is not None
                            and abs(new_weight - old_weight) > weight_threshold
                        )
                    ):
                        holdings_to_update.append(
                            {
                                "ticker": ticker,
                                "holding_ticker": matching_holding["holding_ticker"],
                                "isin": holding,
                                "shares": matching_holding["shares"],
                                "weight": new_weight,
                                "updated_at": datetime.datetime.now(),
                                "name": matching_holding["name"],
                            }
                        )
        # 2-4. 각 그룹 처리
        delete_old_holdings(holdings_to_delete)
        insert_new_holdings(holdings_to_add)
        update_holdings_weights(holdings_to_update)


if __name__ == "__main__":
    try:
        update_kr_etf_holdings()
    except Exception as e:
        logger.error(f"Error in Korean ETF holdings update: {str(e)}", exc_info=True)
        raise
