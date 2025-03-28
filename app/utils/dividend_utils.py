from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List
from app.database.crud import database


class DividendUtils:
    def __init__(self):
        self.db = database

    def get_ttm_dividend_yield(self, tickers: List[str]) -> Dict[str, float]:
        if not tickers:
            return {}

        one_year_ago = datetime.now() - timedelta(days=365)
        ttm_yield_dict = {}

        batch_size = 500
        ttm_dividends_all = []

        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i : i + batch_size]

            aggregates = {"total_dividend": ("per_share", "sum")}

            batch_ttm_dividends = self.db._select(
                table="dividend_information",
                columns=["ticker"],
                aggregates=aggregates,
                group_by=["ticker"],
                ex_date__gte=one_year_ago.strftime("%Y-%m-%d"),
                ticker__in=batch_tickers,
            )

            ttm_dividends_all.extend(batch_ttm_dividends)

        # Get current stock prices
        price_data_all = []

        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i : i + batch_size]

            batch_price_data = self.db._select(
                table="stock_trend", columns=["ticker", "prev_close"], ticker__in=batch_tickers
            )

            price_data_all.extend(batch_price_data)

        # Convert price data to dictionary for faster lookups
        price_dict = {row[0]: row[1] for row in price_data_all}

        # Calculate yields
        for dividend_row in ttm_dividends_all:
            ticker = dividend_row[0]
            ttm_dividend = dividend_row[1]

            if ticker in price_dict and price_dict[ticker] > 0:
                yield_percentage = (ttm_dividend / price_dict[ticker]) * 100
                ttm_yield_dict[ticker] = round(yield_percentage, 2)

        return ttm_yield_dict

    def get_consecutive_dividend_growth(self, tickers: List[str]) -> Dict[str, int]:
        if not tickers:
            return {}

        yearly_dividends = self._get_yearly_dividends(tickers)
        growth_dict = {}

        for ticker, yearly_data in yearly_dividends.items():
            sorted_years = sorted(yearly_data.keys(), reverse=True)

            if len(sorted_years) < 2:
                growth_dict[ticker] = 0
                continue

            consecutive_count = 0

            for i in range(len(sorted_years) - 1):
                current_year = sorted_years[i]
                prev_year = sorted_years[i + 1]

                if yearly_data[current_year] > yearly_data[prev_year]:
                    consecutive_count += 1
                else:
                    break

            growth_dict[ticker] = consecutive_count

        return growth_dict

    def get_consecutive_dividend_payments(self, tickers: List[str]) -> Dict[str, int]:
        if not tickers:
            return {}

        yearly_dividends = self._get_yearly_dividends(tickers)
        payment_dict = {}

        current_year = datetime.now().year

        for ticker, yearly_data in yearly_dividends.items():
            years_with_dividends = set(yearly_data.keys())
            consecutive_count = 0

            for year in range(current_year, current_year - 30, -1):
                if year in years_with_dividends and yearly_data[year] > 0:
                    consecutive_count += 1
                else:
                    break

            payment_dict[ticker] = consecutive_count

        return payment_dict

    def get_dividend_count(self, tickers: List[str]) -> Dict[str, int]:
        if not tickers:
            return {}

        two_years_ago = datetime.now() - timedelta(days=2 * 365)

        batch_size = 500
        dividend_data_all = []

        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i : i + batch_size]

            batch_dividend_data = self.db._select(
                table="dividend_information",
                columns=["ticker", "ex_date", "per_share"],
                ex_date__gte=two_years_ago.strftime("%Y-%m-%d"),
                ticker__in=batch_tickers,
            )

            dividend_data_all.extend(batch_dividend_data)

        ticker_year_counts = defaultdict(lambda: defaultdict(int))

        for record in dividend_data_all:
            ticker = record[0]
            ex_date = record[1]
            amount = record[2]

            if amount is not None and amount > 0:
                year = ex_date.year
                ticker_year_counts[ticker][year] += 1

        frequency_dict = {}

        for ticker, year_counts in ticker_year_counts.items():
            if not year_counts:
                frequency_dict[ticker] = 0
                continue

            total_payments = sum(year_counts.values())
            total_years = len(year_counts)

            if total_years > 0:
                avg_frequency = round(total_payments / total_years)
                frequency_dict[ticker] = avg_frequency
            else:
                frequency_dict[ticker] = 0

        return frequency_dict

    def get_latest_dividend_per_share(self, tickers: List[str]) -> Dict[str, float]:
        if not tickers:
            return {}

        batch_size = 500
        dividend_data_all = []

        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i : i + batch_size]

            batch_dividend_data = self.db._select(
                table="dividend_information",
                columns=["ticker", "per_share", "ex_date"],
                ticker__in=batch_tickers,
                order="ex_date",
                ascending=False,
            )

            dividend_data_all.extend(batch_dividend_data)

        latest_dividend_per_share = {}
        processed_tickers = set()

        for record in dividend_data_all:
            ticker = record[0]
            amount = record[1]

            if ticker not in processed_tickers and amount is not None:
                latest_dividend_per_share[ticker] = amount
                processed_tickers.add(ticker)

        return latest_dividend_per_share

    def _get_yearly_dividends(self, tickers: List[str]) -> Dict[str, Dict[int, float]]:
        batch_size = 500
        dividend_data_all = []

        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i : i + batch_size]

            batch_dividend_data = self.db._select(
                table="dividend_information", columns=["ticker", "ex_date", "per_share"], ticker__in=batch_tickers
            )

            dividend_data_all.extend(batch_dividend_data)

        yearly_dividends = defaultdict(lambda: defaultdict(float))

        for record in dividend_data_all:
            ticker = record[0]
            ex_date = record[1]
            amount = record[2]

            if amount is not None:
                year = ex_date.year
                yearly_dividends[ticker][year] += amount

        return yearly_dividends
