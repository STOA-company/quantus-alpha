from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List
from app.database.crud import database
from collections import Counter


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

    def get_consecutive_dividend_growth_count(self, tickers: List[str]) -> Dict[str, int]:
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

    def get_consecutive_dividend_payment_count(self, tickers: List[str]) -> Dict[str, int]:
        if not tickers:
            return {}

        yearly_dividends = self._get_yearly_dividends(tickers)
        consecutive_dividend_payment_dict = {}

        current_year = datetime.now().year

        for ticker, yearly_data in yearly_dividends.items():
            years_with_dividends = set(yearly_data.keys())
            consecutive_count = 0

            for year in range(current_year, current_year - 30, -1):
                if year in years_with_dividends and yearly_data[year] > 0:
                    consecutive_count += 1
                else:
                    break

            consecutive_dividend_payment_dict[ticker] = consecutive_count

        return consecutive_dividend_payment_dict

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

    def get_dividend_payment_dates(self, tickers: List[str]) -> Dict[str, List[str]]:
        """
        티커별 배당 지급일 리스트를 가져옵니다.

        Args:
            tickers (List[str]): 티커 리스트

        Returns:
            Dict[str, List[str]]: 티커별 배당 지급일 리스트 (YYYY-MM-DD 형식)
        """
        if not tickers:
            return {}

        five_years_ago = datetime.now() - timedelta(days=5 * 365)
        payment_dates_dict = defaultdict(list)

        batch_size = 500
        dividend_data_all = []

        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i : i + batch_size]

            batch_dividend_data = self.db._select(
                table="dividend_information",
                columns=["ticker", "payment_date"],
                payment_date__gte=five_years_ago.strftime("%Y-%m-%d"),
                ticker__in=batch_tickers,
                order="payment_date",
            )

            dividend_data_all.extend(batch_dividend_data)

        for record in dividend_data_all:
            ticker = record[0]
            payment_date = record[1]
            if payment_date:
                payment_dates_dict[ticker].append(payment_date.strftime("%Y-%m-%d"))

        return dict(payment_dates_dict)

    def get_dividend_frequency(self, tickers: List[str]) -> Dict[str, str]:
        """
        티커별 배당 주기를 계산합니다.

        Args:
            tickers (List[str]): 티커 리스트

        Returns:
            Dict[str, str]: 티커별 배당 주기
            - 'week': 주간 배당
            - 'month': 월간 배당
            - 'quarter': 분기 배당
            - 'semi-annual': 반기 배당
            - 'annual': 연간 배당
            - 'no_dividend': 배당금 없음
            - 'insufficient_data': 데이터 부족 (1개의 배당 데이터만 있는 경우)
        """
        if not tickers:
            return {}

        payment_dates_dict = self.get_dividend_payment_dates(tickers)
        frequency_dict = {}

        for ticker, dates in payment_dates_dict.items():
            if not dates:
                frequency_dict[ticker] = "no_dividend"
                continue

            if len(dates) == 1:
                frequency_dict[ticker] = "insufficient_data"
                continue

            # 날짜 정렬
            dates.sort()

            # 날짜 간 간격 계산 (일 단위)
            intervals = []
            for i in range(len(dates) - 1):
                date1 = datetime.strptime(dates[i], "%Y-%m-%d")
                date2 = datetime.strptime(dates[i + 1], "%Y-%m-%d")
                intervals.append((date2 - date1).days)

            if not intervals:
                frequency_dict[ticker] = "insufficient_data"
                continue

            # 평균 간격 계산
            avg_interval = sum(intervals) / len(intervals)

            # 간격의 모드(최빈값) 계산
            interval_counter = Counter(intervals)
            mode_interval = interval_counter.most_common(1)[0][0]

            # 주기 결정 기준 (평균 간격에 기반)
            if avg_interval < 45:  # ~1.5개월
                if avg_interval < 15:  # ~2주
                    cycle = "week"
                else:
                    cycle = "month"
            elif avg_interval < 135:  # ~4.5개월
                cycle = "quarter"
            elif avg_interval < 270:  # ~9개월
                cycle = "semi-annual"
            else:
                cycle = "annual"

            # 최빈값이 평균과 매우 다른 경우, 최빈값 기준으로 재판단
            if abs(mode_interval - avg_interval) > avg_interval * 0.5:
                if mode_interval < 45:
                    if mode_interval < 15:
                        cycle = "week"
                    else:
                        cycle = "month"
                elif mode_interval < 135:
                    cycle = "quarter"
                elif mode_interval < 270:
                    cycle = "semi-annual"
                else:
                    cycle = "annual"

            frequency_dict[ticker] = cycle

        # 배당금이 없는 종목들에 대해 'no_dividend' 설정
        for ticker in tickers:
            if ticker not in frequency_dict:
                frequency_dict[ticker] = "no_dividend"

        return frequency_dict
