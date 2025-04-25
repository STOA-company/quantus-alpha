import datetime
import time

import pandas as pd
import yfinance as yf
from sqlalchemy import text
from yfinance.scrapers.funds import FundsData

from app.core.logger.logger import setup_logger
from app.database.crud import database
from app.kispy.manager import KISAPIManager

logger = setup_logger("fund_analysis", level="DEBUG")
api = KISAPIManager().get_api()


def get_db_etf_list(ctry: str = "US"):
    """
    Get a list of ETFs from the database.

    Args:
        ctry (str): Country code ("US" or "KR")
    """
    logger.info(f"Fetching {ctry} ETF list from database")
    etf_list = database._select(
        table="stock_information",
        columns=["ticker"],
        ctry=ctry,
        type="ETF",
        is_delisted=False,
    )
    etf_list = [row.ticker for row in etf_list]
    logger.info(f"Retrieved {len(etf_list)} {ctry} ETFs from database")
    return etf_list


def get_kr_etf_top_holdings(symbol: str):
    """ """
    logger.debug(f"Fetching top holdings for Korean ETF {symbol}")
    try:
        if symbol.startswith("A"):
            symbol = symbol[1:]
        top_holdings = ""
        return top_holdings

    except Exception as e:
        logger.error(f"Error fetching holdings for {symbol}: {str(e)}", exc_info=True)
        return None


def get_us_etf_top_holdings(symbol: str):
    """
    Get top holdings information for a US ETF using yfinance.

    Args:
        symbol (str): The ETF symbol (e.g., 'SPY', 'QQQ')

    Returns:
        pd.DataFrame: DataFrame containing top holdings information, or None if data is not available
    """
    logger.debug(f"Fetching top holdings for US ETF {symbol}")
    try:
        ticker = yf.Ticker(symbol)

        if ticker.info is None or len(ticker.info) == 0:
            logger.warning(f"No data available for {symbol}")
            return None

        fund_data = FundsData(ticker._data, symbol)
        top_holdings = fund_data.top_holdings

        if top_holdings is None or top_holdings.empty:
            logger.warning(f"No holdings data found for {symbol}")
            return None

        logger.debug(f"Successfully retrieved holdings for {symbol} \n {top_holdings}")
        return top_holdings

    except Exception as e:
        logger.error(f"Error fetching holdings for {symbol}: {str(e)}", exc_info=True)
        return None


def process_top_holdings(top_holdings: pd.DataFrame, symbol: str, ctry: str = "US"):
    """
    Process the top holdings DataFrame by cleaning and formatting it.

    Args:
        top_holdings (pd.DataFrame): Raw holdings data
        symbol (str): ETF symbol

    Returns:
        pd.DataFrame: Processed holdings data
    """
    logger.debug(f"top_holdings.columns: {top_holdings.columns}")

    # Reset index to make Symbol a column
    top_holdings = top_holdings.reset_index()

    # Rename columns to match database schema
    top_holdings = top_holdings.rename(columns={"Symbol": "holding_ticker", "Holding Percent": "weight", "Name": "name"})

    # Add metadata columns
    top_holdings["ticker"] = symbol
    top_holdings["updated_at"] = datetime.datetime.now()

    # Filter out records with empty top_holdings
    top_holdings = top_holdings[top_holdings["holding_ticker"].str.strip() != ""]

    # Standardize ticker formats
    def standardize_ticker(ticker):
        if ticker.endswith(".KS"):
            return "A" + ticker[:-3]
        elif ticker.endswith(".HK"):
            return "HK" + ticker[:-3]
        elif ticker.endswith(".SW"):
            return "CH" + ticker[:-3]
        return ticker

    top_holdings["holding_ticker"] = top_holdings["holding_ticker"].apply(standardize_ticker)

    top_holdings = top_holdings[["ticker", "holding_ticker", "weight", "updated_at", "name"]]

    logger.debug(f"Processed holdings for {symbol}: {len(top_holdings)} records")
    return top_holdings


def get_existing_holdings(chunk_etfs: list):
    """
    Get existing holdings from the database.
    Returns a set of tuples (ticker, top_holdings, weight) for quick lookup.
    """
    existing_data = database._select(
        table="etf_top_holdings", columns=["ticker", "holding_ticker", "weight"], ticker__in=chunk_etfs
    )

    # Create a set of existing holdings including weight
    existing_holdings = {(row.ticker, row.holding_ticker, row.weight) for row in existing_data}

    logger.info(f"Retrieved {len(existing_holdings)} existing holdings from database")
    return existing_holdings


def fetch_new_holdings(etf_list: list, ctry: str = "US", chunk_size: int = 10) -> tuple[set, set]:
    """
    Fetch new holdings data from yfinance/KIS API for all ETFs.

    Args:
        etf_list: List of ETF symbols
        ctry (str): Country code ("US" or "KR")
        chunk_size: Number of ETFs to process in each chunk

    Returns:
        tuple: (new_holdings_keys, new_holdings)
            - new_holdings_keys: Set of (ticker, top_holdings) tuples
            - new_holdings: Set of complete holding records
    """
    logger.info(f"Starting to fetch holdings for {len(etf_list)} ETFs")
    new_holdings_keys = set()
    new_holdings = set()
    total_etfs = len(etf_list)

    for i in range(0, total_etfs, chunk_size):
        chunk_etfs = etf_list[i : i + chunk_size]
        logger.info(f"Processing chunk {i//chunk_size + 1} of {(total_etfs + chunk_size - 1)//chunk_size}")

        for symbol in chunk_etfs:
            try:
                top_holdings = get_us_etf_top_holdings(symbol)
                logger.debug(f"top_holdings: \n {top_holdings}")
                if top_holdings is not None and not top_holdings.empty:
                    top_holdings = process_top_holdings(top_holdings, symbol, ctry)
                    logger.debug(f"top_holdings: \n {top_holdings}")
                    # Add to holdings sets
                    for _, row in top_holdings.iterrows():
                        holding_key = (row["ticker"], row["holding_ticker"])
                        new_holdings_keys.add(holding_key)
                        new_holdings.add(tuple(row))

            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}", exc_info=True)
                continue

            # 100개마다 1초 쉬기
            if len(new_holdings) % 100 == 0:
                logger.debug(f"Processed {len(new_holdings)} holdings so far")
                time.sleep(1)

        # 청크 사이에 잠시 대기
        time.sleep(1)

    logger.info(f"Completed fetching holdings. Total holdings processed: {len(new_holdings)}")
    return new_holdings_keys, new_holdings


def delete_old_holdings(holdings_to_delete: set) -> None:
    """
    Delete holdings that no longer exist in the new data.

    Args:
        holdings_to_delete: Set of (ticker, top_holdings) tuples to delete
    """
    if not holdings_to_delete:
        logger.info("No holdings to delete")
        return

    logger.info(f"Preparing to delete {len(holdings_to_delete)} old holdings")
    delete_conditions = []
    for ticker, holding in holdings_to_delete:
        delete_conditions.append(f"(ticker = '{ticker}' AND top_holdings = '{holding}')")

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


def insert_new_holdings(holdings_to_add: set, chunk_size: int = 1000) -> None:
    """
    Insert new holdings into the database.

    Args:
        holdings_to_add: Set of complete holding records to add
        chunk_size: Number of records to insert in each chunk
    """
    if not holdings_to_add:
        logger.info("No new holdings to insert")
        return

    logger.info(f"Preparing to insert {len(holdings_to_add)} new holdings")
    # Convert set of tuples to list of dictionaries
    new_data = []
    for holding in holdings_to_add:
        new_data.append(
            {
                "ticker": holding[0],  # ETF ticker
                "holding_ticker": holding[1],  # Constituent ticker
                "weight": holding[2],  # Weight
                "updated_at": holding[3],  # Timestamp
                "name": holding[4],  # Name
            }
        )

    # Insert in chunks to avoid too large transactions
    for i in range(0, len(new_data), chunk_size):
        chunk = new_data[i : i + chunk_size]
        try:
            database._insert(table="etf_top_holdings", sets=chunk)
            logger.debug(f"Inserted chunk {i//chunk_size + 1} of {(len(new_data) + chunk_size - 1)//chunk_size}")
        except Exception as e:
            logger.error(f"Error inserting chunk {i//chunk_size + 1}: {str(e)}", exc_info=True)
            raise

    logger.info(f"Successfully added {len(holdings_to_add)} new holdings")


def update_holdings_weights(holdings_to_update: list, chunk_size: int = 1000) -> None:
    """
    Update weights for holdings where the weight has changed.

    Args:
        holdings_to_update: List of dictionaries with updated holding information
        chunk_size: Number of records to update in each chunk
    """
    if not holdings_to_update:
        logger.info("No holdings weights to update")
        return

    logger.info(f"Preparing to update weights for {len(holdings_to_update)} holdings")

    # Process in chunks to avoid too large transactions
    for i in range(0, len(holdings_to_update), chunk_size):
        chunk = holdings_to_update[i : i + chunk_size]
        try:
            # Update each holding in the chunk
            for holding in chunk:
                database._update(
                    table="etf_top_holdings",
                    sets=holding,
                    ticker=holding["ticker"],
                    holding_ticker=holding["holding_ticker"],
                )
            logger.debug(f"Updated chunk {i//chunk_size + 1} of {(len(holdings_to_update) + chunk_size - 1)//chunk_size}")
        except Exception as e:
            logger.error(f"Error updating weights for chunk {i//chunk_size + 1}: {str(e)}", exc_info=True)
            raise

    logger.info(f"Successfully updated weights for {len(holdings_to_update)} holdings")


def update_etf_top_holdings(chunk_size: int = 10, ctry: str = "US", weight_threshold: float = 0.01):
    """
    Update the top holdings for all ETFs in the database.
    This function:
    1. Fetches new data from yfinance/KIS API in chunks
    2. Compares with existing data in DB
    3. Deletes holdings that no longer exist
    4. Updates holdings where weight has changed
    5. Adds new holdings

    Args:
        chunk_size (int): Number of ETFs to process in each chunk
        ctry (str): Country code ("US" or "KR")
        weight_threshold (float): Minimum weight difference to trigger an update (in percentage points)
    """
    logger.info(f"Starting {ctry} ETF top holdings update process")

    # Get ETF list
    etf_list = get_db_etf_list(ctry)
    # etf_list = ["QQQ"]
    total_etfs = len(etf_list)
    logger.info(f"Total {ctry} ETFs to process: {total_etfs}")

    # Process ETFs in chunks
    for i in range(0, total_etfs, chunk_size):
        chunk_etfs = etf_list[i : i + chunk_size]
        logger.info(f"Processing chunk {i//chunk_size + 1} of {(total_etfs + chunk_size - 1)//chunk_size}")

        # Get existing holdings for current chunk
        existing_holdings = get_existing_holdings(chunk_etfs)
        logger.debug(f"existing_holdings: \n {existing_holdings}")
        logger.info(f"Retrieved {len(existing_holdings)} existing holdings from database")

        # Fetch new holdings for current chunk
        new_holdings_keys, new_holdings = fetch_new_holdings(chunk_etfs, ctry=ctry)

        # Create a dictionary of new holdings for easy weight comparison
        new_holdings_dict = {(h[0], h[1]): {"weight": h[2], "name": h[4]} for h in new_holdings}

        # Determine what needs to be deleted and added
        existing_keys = {h[:2] for h in existing_holdings}
        holdings_to_delete = existing_keys - new_holdings_keys
        holdings_to_add = {h for h in new_holdings if (h[0], h[1]) not in existing_keys}

        # Find holdings with changed weights
        holdings_to_update = []
        for ticker, holding, old_weight in existing_holdings:
            if (ticker, holding) in new_holdings_dict:
                new_weight = new_holdings_dict[(ticker, holding)]["weight"]
                # Only update if the weight difference exceeds the threshold
                if abs(new_weight - old_weight) > weight_threshold:
                    holdings_to_update.append(
                        {
                            "ticker": ticker,
                            "holding_ticker": holding,
                            "weight": new_weight,
                            "updated_at": datetime.datetime.now(),
                            "name": new_holdings_dict[(ticker, holding)]["name"],
                        }
                    )

        logger.info(
            f"Chunk update summary: {len(holdings_to_delete)} holdings to delete, "
            f"{len(holdings_to_update)} holdings to update, {len(holdings_to_add)} holdings to add"
        )

        # Delete old holdings
        delete_old_holdings(holdings_to_delete)

        # Update weights for existing holdings
        update_holdings_weights(holdings_to_update)

        # Insert new holdings
        insert_new_holdings(holdings_to_add)

    logger.info(f"{ctry} ETF top holdings update completed successfully")


if __name__ == "__main__":
    try:
        # Update US ETFs
        update_etf_top_holdings(ctry="US")
        # # Update Korean ETFs
        # update_etf_top_holdings(ctry="KR")
    except Exception as e:
        logger.error(f"Error in ETF top holdings update: {str(e)}", exc_info=True)
        raise
