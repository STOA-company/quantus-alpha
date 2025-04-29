from datetime import timedelta
from typing import List, Literal

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.cache.leaderboard import DisclosureLeaderboard, NewsLeaderboard
from app.core.exception.base import DuplicateException, NotFoundException
from app.core.logger.logger.base import setup_logger
from app.database.crud import database, database_service
from app.modules.common.enum import TranslateCountry
from app.modules.news.schemas import DisclosureRenewalItem, NewsRenewalItem
from app.modules.news.services import get_news_service
from app.utils.ctry_utils import check_ticker_country_len_2
from app.utils.date_utils import now_utc

logger = setup_logger(__name__)


class InterestService:
    def __init__(self):
        self.db = database_service
        self.data_db = database

    def get_name(self, row, lang: TranslateCountry) -> str:
        """
        언어에 따라 적절한 이름을 반환합니다.

        Args:
            row: 이름 정보가 포함된 행 객체
            lang: 언어 설정

        Returns:
            str: 선택된 언어의 이름
        """
        return row.kr_name if lang == TranslateCountry.KO else row.en_name

    def get_interest_tickers(self, group_id: int):
        group = self.db._select(table="alphafinder_interest_group", columns=["name"], id=group_id)
        if not group:
            raise NotFoundException(message="관심 종목 그룹이 존재하지 않습니다.")

        if group[0].name == "실시간 인기":
            current_datetime = now_utc()
            before_24_hours = current_datetime - timedelta(hours=24)
            allowed_time = current_datetime + timedelta(minutes=5)
            query_us = f"""
                SELECT st.ticker
                FROM quantus.stock_trend st
                JOIN (
                    SELECT DISTINCT ticker
                    FROM quantus.news_analysis
                    WHERE date >= '{before_24_hours}'
                    AND date <= '{allowed_time}'
                    AND is_related = TRUE
                    AND is_exist = TRUE
                ) na ON st.ticker = na.ticker
                WHERE ctry = 'US'
                ORDER BY st.volume_change_rt DESC
                LIMIT 6
            """
            top_stories_data_us = self.db._execute(text(query_us))
            query_kr = f"""
                SELECT st.ticker
                FROM quantus.stock_trend st
                JOIN (
                    SELECT DISTINCT ticker
                    FROM quantus.news_analysis
                    WHERE date >= '{before_24_hours}'
                    AND date <= '{allowed_time}'
                    AND is_related = TRUE
                    AND is_exist = TRUE
                ) na ON st.ticker = na.ticker
                WHERE ctry = 'KR'
                ORDER BY st.volume_change_rt DESC
                LIMIT 5
            """
            top_stories_data_kr = self.db._execute(text(query_kr))

            top_stories_tickers = set()

            for row in top_stories_data_us:
                ticker = row[0]
                top_stories_tickers.add(ticker)

            for row in top_stories_data_kr:
                ticker = row[0]
                top_stories_tickers.add(ticker)

            interests = list(top_stories_tickers)

        else:
            interests = self.db._select(table="alphafinder_interest_stock", columns=["ticker"], group_id=group_id)
            if not interests:
                return []
            interests = [interest.ticker for interest in interests]

        return interests

    def add_interest(self, group_id: int, ticker: str, user_id: int):
        try:
            # Check group ownership and editability
            group = self.db._select(
                table="alphafinder_interest_group", columns=["user_id", "is_editable"], id=group_id, limit=1
            )
            if not group:
                raise NotFoundException(message="관심 종목 그룹이 존재하지 않습니다.")
            if group[0].user_id != user_id:
                raise HTTPException(status_code=400, detail="관심 그룹 수정 권한이 없습니다.")
            if group[0].is_editable == 0:
                raise HTTPException(status_code=400, detail="수정 불가능한 그룹입니다.")

            # Check if ticker already exists in the group
            existing_interest = self.db._select(
                table="alphafinder_interest_stock", columns=["ticker"], group_id=group_id, ticker=ticker, limit=1
            )
            if existing_interest:
                raise DuplicateException(message="이미 관심 종목에 추가되어 있습니다.")

            # Get the maximum order value for the group's interests
            max_order = self.db._execute(
                text("SELECT MAX(`order`) as max_order FROM alphafinder_interest_stock WHERE group_id = :group_id"),
                {"group_id": group_id},
            )
            max_order_row = max_order.fetchone()
            next_order = (max_order_row[0] if max_order_row and max_order_row[0] is not None else 0) + 1

            # Add the interest with order
            result = self.db._insert(
                table="alphafinder_interest_stock", sets={"group_id": group_id, "ticker": ticker, "order": next_order}
            )

            return result.lastrowid

        except NotFoundException as e:
            raise HTTPException(status_code=e.status_code, detail=e.message)
        except DuplicateException as e:
            raise HTTPException(status_code=e.status_code, detail=e.message)
        except Exception as e:
            logger.exception(e)
            raise HTTPException(status_code=500, detail=str(e))

    def delete_interest(self, group_id: int, tickers: List[str], user_id: int):
        try:
            # Check group ownership and editability
            group = self.db._select(
                table="alphafinder_interest_group", columns=["user_id", "is_editable"], id=group_id, limit=1
            )
            if not group:
                raise NotFoundException(message="관심 종목 그룹이 존재하지 않습니다.")
            if group[0].user_id != user_id:
                raise HTTPException(status_code=400, detail="관심 그룹 수정 권한이 없습니다.")
            if group[0].is_editable is False:
                raise HTTPException(status_code=400, detail="수정 불가능한 그룹입니다.")

            # Check if all tickers exist in the group
            for ticker in tickers:
                existing_interest = self.db._select(
                    table="alphafinder_interest_stock", group_id=group_id, ticker=ticker, limit=1
                )
                if not existing_interest:
                    raise NotFoundException(message=f"관심 종목 {ticker}이(가) 그룹에 존재하지 않습니다.")

            # Delete all tickers in a single query
            self.db._delete(table="alphafinder_interest_stock", group_id=group_id, ticker__in=tickers)

            return True

        except NotFoundException as e:
            raise HTTPException(status_code=e.status_code, detail=e.message)
        except Exception as e:
            logger.exception(e)
            raise HTTPException(status_code=500, detail=str(e))

    def update_interest(self, user_id: int, group_ids: List[int], ticker: str):
        # 빈 배열이 들어오면 모든 그룹에서 해당 티커 삭제
        if not group_ids:
            # 1. 사용자의 모든 그룹에서 해당 티커가 있는지 확인
            query = """
                SELECT group_id
                FROM alphafinder_interest_stock
                WHERE ticker = :ticker
                AND group_id IN (
                    SELECT id
                    FROM alphafinder_interest_group
                    WHERE user_id = :user_id
                )
            """
            result = self.db._execute(text(query), {"ticker": ticker, "user_id": user_id})
            groups_to_remove = [row[0] for row in result.fetchall()]

            # 2. 해당 티커가 있는 모든 그룹에서 삭제
            if groups_to_remove:
                self.db._delete(table="alphafinder_interest_stock", ticker=ticker, group_id__in=groups_to_remove)

            return True

        # 1. Get current status of all groups in a single query
        query = """
            WITH user_groups AS (
                SELECT id
                FROM alphafinder_interest_group
                WHERE user_id = :user_id
            ),
            current_groups AS (
                SELECT group_id
                FROM alphafinder_interest_stock
                WHERE ticker = :ticker
                AND group_id IN (SELECT id FROM user_groups)
            ),
            requested_groups AS (
                SELECT id as group_id
                FROM user_groups
                WHERE id IN :group_ids
            )
            SELECT
                CASE
                    WHEN c.group_id IS NOT NULL THEN 'current'
                    ELSE 'requested'
                END as status,
                r.group_id
            FROM requested_groups r
            LEFT JOIN current_groups c ON r.group_id = c.group_id
            UNION
            SELECT
                'current' as status,
                c.group_id
            FROM current_groups c
            LEFT JOIN requested_groups r ON c.group_id = r.group_id
            WHERE r.group_id IS NULL
        """

        result = self.db._execute(text(query), {"ticker": ticker, "user_id": user_id, "group_ids": tuple(group_ids)})

        # 2. Process results
        groups_to_remove = set()
        groups_to_add = set()
        max_orders = {}

        for row in result.fetchall():
            if row.status == "current" and row.group_id not in group_ids:
                print(f"row.group_id : {row}")
                groups_to_remove.add(row.group_id)
            elif row.status == "requested" and row.group_id not in groups_to_remove:
                print(f"row.group_id : {row}")
                groups_to_add.add(row.group_id)

        # 3. Get max orders for groups to add
        if groups_to_add:
            query = """
                SELECT group_id, MAX(`order`) as max_order
                FROM alphafinder_interest_stock
                WHERE group_id IN :group_ids
                GROUP BY group_id
            """
            result = self.db._execute(text(query), {"group_ids": tuple(groups_to_add)})
            max_orders = {row[0]: (row[1] or 0) for row in result.fetchall()}

        # 4. Execute updates
        if groups_to_remove:
            self.db._delete(table="alphafinder_interest_stock", ticker=ticker, group_id__in=list(groups_to_remove))

        if groups_to_add:
            insert_data = [
                {"group_id": group_id, "ticker": ticker, "order": max_orders.get(group_id, 0) + 1}
                for group_id in groups_to_add
            ]
            self.db._insert(table="alphafinder_interest_stock", sets=insert_data)

        return True

    def get_interest_list(self, user_id: int, lang: TranslateCountry):
        # 1. Get interest groups and their tickers from db
        query = """
            SELECT
                g.id,
                g.name,
                g.order as group_order,
                g.is_editable,
                i.ticker,
                i.order as stock_order
            FROM
                alphafinder_interest_group g
            LEFT JOIN
                alphafinder_interest_stock i ON g.id = i.group_id
            WHERE
                g.user_id = :user_id
            ORDER BY
                g.order ASC, i.order ASC
        """

        result = self.db._execute(text(query), {"user_id": user_id})
        rows = result.fetchall()

        # 2. Extract all unique tickers
        tickers = {row.ticker for row in rows if row.ticker}

        # 3. Get stock information from data_db
        if tickers:
            if lang == TranslateCountry.KO:
                name_column = "kr_name"
            else:
                name_column = "en_name"

            stock_info_query = f"""
                SELECT ticker, {name_column}, ctry
                FROM stock_information
                WHERE ticker IN :tickers
            """
            stock_info_result = self.data_db._execute(text(stock_info_query), {"tickers": tuple(tickers)})
            stock_info = {
                row.ticker: {
                    "name": getattr(row, name_column) or row.ticker,  # If name is None, use ticker as fallback
                    "ctry": row.ctry or check_ticker_country_len_2(row.ticker),
                }
                for row in stock_info_result.fetchall()
            }

        # 4. Group the results
        groups = {}
        for row in rows:
            group_id = row.id
            if group_id not in groups:
                groups[group_id] = {
                    "id": group_id,
                    "name": row.name,
                    "is_editable": row.is_editable,
                    "stocks": [],
                    "order": row.group_order,
                }
            if row.ticker and row.ticker in stock_info:  # Only add if we have stock info
                ticker_data = {
                    "ticker": row.ticker,
                    "name": stock_info[row.ticker]["name"],
                    "ctry": stock_info[row.ticker]["ctry"],
                    "order": row.stock_order,
                }
                groups[group_id]["stocks"].append(ticker_data)

        # 5. Convert to list and sort by group order
        groups_list = list(groups.values())
        groups_list.sort(key=lambda x: x["order"])

        # 6. Sort stocks within each group by their order and remove order field
        for group in groups_list:
            group["stocks"].sort(key=lambda x: x["order"])
            # Remove order field from stocks
            for stock in group["stocks"]:
                del stock["order"]
            # Remove order field from group
            del group["order"]

        return groups_list

    def get_interest_group(self, user_id: int):
        """
        사용자의 관심 그룹 목록을 조회합니다.

        Args:
            user_id (int): 사용자의 고유 ID

        Returns:
            List[Dict]: 관심 그룹 목록
                - 각 그룹은 id, name, count를 포함
                - 그룹은 order 필드 기준 오름차순으로 정렬됨

        Note:
            - 사용자가 처음 접속하는 경우, 기본 그룹("실시간 인기")이 자동으로 생성됨
            - "실시간 인기" 그룹은 필수 그룹으로, 삭제할 수 없음
        """
        groups = self.db._select(table="alphafinder_interest_group", user_id=user_id, order="order", ascending=True)
        if not groups or not any(group.name in ["실시간 인기"] for group in groups):
            return self.init_interest_group(user_id)
        result = [
            {
                "id": group.id,
                "name": group.name,
                "count": 11 if group.name == "실시간 인기" else self.get_interest_count(group.id),
            }
            for group in groups
        ]
        return result

    def get_interest_group_by_ticker(self, user_id: int, ticker: str):
        query = """
            SELECT
                g.id,
                g.name,
                CASE WHEN EXISTS (
                    SELECT 1 FROM alphafinder_interest_stock
                    WHERE group_id = g.id AND ticker = :ticker
                ) THEN 1 ELSE 0 END as has_ticker
            FROM alphafinder_interest_group g
            LEFT JOIN alphafinder_interest_stock i ON g.id = i.group_id
            WHERE g.user_id = :user_id
            GROUP BY g.id, g.name
            ORDER BY g.order ASC
        """

        result = self.db._execute(text(query), {"user_id": user_id, "ticker": ticker})
        rows = result.fetchall()

        if not rows:
            return []

        return [
            {"id": row.id, "name": row.name, "has_ticker": bool(row.has_ticker)}
            for row in rows
            if row.name != "실시간 인기"
        ]

    def init_interest_group(self, user_id: int):
        """
        사용자의 기본 관심 그룹을 초기화합니다.

        Args:
            user_id (int): 사용자의 고유 ID

        Returns:
            List[Dict]: 초기화된 관심 그룹 목록

        Note:
            - "실시간 인기" 그룹이 없는 경우에만 생성됨
            - 생성된 그룹은 order=0, is_editable=False로 설정됨
        """
        # Check existing groups
        existing_groups = self.db._select(
            table="alphafinder_interest_group", user_id=user_id, name__in=["실시간 인기", "기본"]
        )
        existing_names = {group.name for group in existing_groups}

        # Insert missing groups
        if "실시간 인기" not in existing_names:
            self.db._insert(
                table="alphafinder_interest_group",
                sets={"name": "실시간 인기", "user_id": user_id, "order": 0, "is_editable": False},
            )

        # Return all groups
        groups = self.db._select(table="alphafinder_interest_group", user_id=user_id, order="order", ascending=True)
        return [{"id": group.id, "name": group.name} for group in groups]

    def create_interest_group(self, user_id: int, name: str):
        """
        새로운 관심 그룹을 생성합니다.

        Args:
            user_id (int): 사용자의 고유 ID
            name (str): 생성할 그룹의 이름

        Returns:
            int: 생성된 그룹의 ID

        Raises:
            DuplicateException: 동일한 이름의 그룹이 이미 존재하는 경우
            HTTPException: 데이터베이스 오류가 발생한 경우
        """
        try:
            group = self.db._select(table="alphafinder_interest_group", user_id=user_id, name=name, limit=1)
            if group:
                raise DuplicateException(message="이미 존재하는 관심 그룹입니다.")

            # Get max order using raw SQL query
            query = "SELECT MAX(`order`) as max_order FROM alphafinder_interest_group WHERE user_id = :user_id"
            result = self.db._execute(text(query), {"user_id": user_id})
            max_order_row = result.fetchone()
            next_order = (max_order_row[0] if max_order_row and max_order_row[0] is not None else 0) + 1

            result = self.db._insert(
                table="alphafinder_interest_group",
                sets={"user_id": user_id, "name": name, "order": next_order, "is_editable": 1},
            )
            return result.lastrowid
        except IntegrityError:
            raise HTTPException(status_code=409, detail="이미 사용 중인 그룹 이름입니다.")
        except DuplicateException as e:
            raise HTTPException(status_code=e.status_code, detail=e.message)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    def delete_interest_group(self, group_id: int, user_id: int):
        """
        기존 관심 그룹을 삭제합니다.

        Args:
            group_id (int): 삭제할 그룹의 ID
            user_id (int): 사용자의 고유 ID

        Returns:
            bool: 삭제 성공 여부

        Raises:
            HTTPException:
                - 그룹이 존재하지 않는 경우 (404)
                - 사용자가 그룹의 소유자가 아닌 경우 (400)
                - 수정 불가능한 그룹인 경우 (400)
                - 데이터베이스 오류가 발생한 경우 (500)
        """
        try:
            group = self.db._select(
                table="alphafinder_interest_group", columns=["user_id", "is_editable"], id=group_id, limit=1
            )
            print(f"group : {group}")
            print(f"group[0] : {group[0]}")
            if group[0].user_id != user_id:
                raise HTTPException(status_code=400, detail="관심 그룹 삭제 권한이 없습니다.")
            if group[0].is_editable == 0:
                raise HTTPException(status_code=400, detail="수정 불가능한 그룹입니다.")
            self.db._delete(table="alphafinder_interest_group", id=group_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return True

    def update_interest_group_name(self, group_id: int, name: str, user_id: int):
        """
        관심 그룹의 이름을 수정합니다.

        Args:
            group_id (int): 수정할 그룹의 ID
            name (str): 새로운 그룹 이름
            user_id (int): 사용자의 고유 ID

        Returns:
            bool: 수정 성공 여부

        Raises:
            HTTPException:
                - 그룹이 존재하지 않는 경우 (404)
                - 사용자가 그룹의 소유자가 아닌 경우 (400)
                - 수정 불가능한 그룹인 경우 (400)
                - 동일한 이름의 그룹이 이미 존재하는 경우 (409)
                - 데이터베이스 오류가 발생한 경우 (500)
        """
        try:
            group = self.db._select(
                table="alphafinder_interest_group", columns=["name", "user_id", "is_editable"], id=group_id, limit=1
            )
            if group[0].is_editable == 0:
                raise HTTPException(status_code=400, detail="수정 불가능한 그룹입니다.")
            if group[0].user_id != user_id:
                raise HTTPException(status_code=400, detail="관심 그룹 수정 권한이 없습니다.")
            if not group:
                raise NotFoundException(message="관심 그룹이 존재하지 않습니다.")
            if name == group[0].name:
                raise DuplicateException(message="기존 이름과 동일합니다.")
            update_time = now_utc()
            self.db._update(
                table="alphafinder_interest_group", id=group_id, sets={"name": name, "updated_at": update_time}
            )
            return True

        except NotFoundException as e:
            raise HTTPException(status_code=e.status_code, detail=e.message)
        except DuplicateException as e:
            raise HTTPException(status_code=e.status_code, detail=e.message)
        except IntegrityError:
            raise HTTPException(status_code=409, detail="이미 사용 중인 그룹 이름입니다.")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    def get_won_unit(self, number, lang):
        if isinstance(number, str):
            number = int(number.replace(",", ""))

        if lang == "ko":
            if number < 100000000:  # 1억 미만
                return (number, "원")
            elif number < 1000000000000:  # 1조 미만
                return (round(float(number / 100000000), 0), "억원")
            elif number < 10000000000000000:  # 1경 미만
                return (round(float(number / 1000000000000), 2), "조원")
            else:
                return (round(float(number / 10000000000000000), 0), "경원")
        else:
            if number < 1000000:  # 1K 미만
                return (number, "₩")
            elif number < 1000000000000:  # 1T 미만
                return (float(number / 1000000), "B₩")
            else:
                return (float(number / 1000000000000), "T₩")

    def get_dollar_unit(self, number):
        if isinstance(number, str):
            number = float(number.replace(",", ""))

        if number < 1000:  # 1K 미만
            return (number, "$")
        elif number < 1000000:  # 1M 미만
            return (float(number / 1000), "K$")
        elif number < 1000000000:  # 1B 미만
            return (float(number / 1000000), "M$")
        elif number < 1000000000000:  # 1T 미만
            return (float(number / 1000000000), "B$")
        else:
            return (float(number / 1000000000000), "T$")

    def get_interest_count(self, group_id: int):
        count = self.db._count(table="alphafinder_interest_stock", group_id=group_id)
        return count

    def get_interest_news_leaderboard(
        self,
        group_id: int,
        lang: Literal["ko", "en"] = "ko",
        subscription_level: int = 1,
    ) -> List[NewsRenewalItem]:
        redis = NewsLeaderboard()
        news_service = get_news_service()
        ticker_infos = self.get_interest_tickers(group_id)
        if len(ticker_infos) == 0:
            return []
        tickers = [ticker_info["ticker"] for ticker_info in ticker_infos]
        leaderboard_data = redis.get_leaderboard(tickers=tickers)[:5]
        news_ids = [item.get("news_id") for item in leaderboard_data]
        news_items = news_service.get_news_by_id(news_ids, lang)
        if news_items is None:
            return []
        news_tickers = [item.ticker for item in news_items]
        # 구독 레벨이 3 미만인 경우에만 마스킹 적용
        if subscription_level < 3 and news_items:
            # 각 티커별 최신 10개 뉴스 ID 조회
            recent_news_ids = news_service.get_recent_news_ids_by_ticker(news_tickers, limit=10, lang=lang)

            # 티커별 ID를 이용한 최적화된 마스킹 적용
            news_items = news_service.mask_news_items_by_id(news_items, recent_news_ids)
        return news_items

    def get_interest_disclosure_leaderboard(
        self,
        group_id: int,
        lang: Literal["ko", "en"] = "ko",
        subscription_level: int = 1,
    ) -> List[DisclosureRenewalItem]:
        redis = DisclosureLeaderboard()
        news_service = get_news_service()
        ticker_infos = self.get_interest_tickers(group_id)
        if len(ticker_infos) == 0:
            return []
        tickers = [ticker_info["ticker"] for ticker_info in ticker_infos]
        leaderboard_data = redis.get_leaderboard(tickers=tickers)[:5]
        disclosure_ids = [item.get("disclosure_id") for item in leaderboard_data]
        disclosure_items = news_service.get_disclosure_by_id(disclosure_ids, lang)

        # 구독 레벨이 3 미만인 경우에만 마스킹 적용
        if subscription_level < 3 and disclosure_items:
            # 날짜 기반 마스킹 적용 (7일 이전 데이터 마스킹)
            return news_service.mask_disclosure_items_by_date(disclosure_items)

        return disclosure_items

    def get_interest_info(self, user_id: int, ticker: str):
        query = self.db._select(table="interest_group", user_id=user_id)
        if not query:
            return {"is_interested": False, "groups": []}

        total_groups = [
            {
                "id": group.id,
                "name": group.name,
                "included": True
                if self.db._select(table="user_stock_interest", group_id=group.id, ticker=ticker)
                else False,
            }
            for group in query
        ]

        for group in total_groups:
            if group["included"] is True:
                return {"is_interested": True, "groups": total_groups}

        return {"is_interested": False, "groups": total_groups}

    def update_order(self, user_id: int, group_id: int | None, order_list: List[int] | List[str]):
        try:
            if group_id is None or group_id == 0:
                # 그룹 순서 변경
                # 모든 그룹이 사용자의 것인지 확인
                groups = self.db._select(table="alphafinder_interest_group", user_id=user_id, id__in=order_list)
                if len(groups) != len(order_list):
                    raise HTTPException(status_code=400, detail="잘못된 그룹 ID가 포함되어 있습니다.")

                # 그룹 순서 업데이트
                for idx, group_id in enumerate(order_list, 1):
                    self.db._update(table="alphafinder_interest_group", id=group_id, sets={"order": idx})
            else:
                # 종목 순서 변경
                # 그룹 소유권 확인
                group = self.db._select(table="alphafinder_interest_group", id=group_id, user_id=user_id, limit=1)
                if not group:
                    raise HTTPException(status_code=400, detail="관심 그룹이 존재하지 않습니다.")

                # 모든 종목이 그룹에 속한 것인지 확인
                interests = self.db._select(table="alphafinder_interest_stock", group_id=group_id, ticker__in=order_list)
                if len(interests) != len(order_list):
                    raise HTTPException(status_code=400, detail="기록된 종목의 갯수와 맞지 않습니다.")

                # 종목 순서 업데이트
                for idx, ticker in enumerate(order_list, 1):
                    self.db._update(
                        table="alphafinder_interest_stock", group_id=group_id, ticker=ticker, sets={"order": idx}
                    )

            return True

        except HTTPException as e:
            raise e
        except Exception as e:
            logger.exception(e)
            raise HTTPException(status_code=500, detail=str(e))

    def move_interest(self, from_group_id: int, to_group_id: int, tickers: List[str], user_id: int):
        # 유저 권한 체크
        from_group = self.db._select(table="alphafinder_interest_group", columns=["user_id"], id=from_group_id, limit=1)
        if not from_group:
            raise HTTPException(status_code=400, detail="관심 그룹이 존재하지 않습니다.")
        if from_group[0].user_id != user_id:
            raise HTTPException(status_code=403, detail="관심 그룹 편집 권한이 없습니다.")

        # 1. 티커들이 해당 종목에 있는지 확인
        from_group_tickers = self.db._select(
            table="alphafinder_interest_stock", group_id=from_group_id, ticker__in=tickers
        )
        if len(from_group_tickers) != len(tickers):
            raise HTTPException(status_code=400, detail="관심 그룹에 존재하지 않는 종목이 포함되어 있습니다.")

        # 2. 해당 종목이 이동하는 그룹에 있는지 확인
        to_group_tickers = self.db._select(table="alphafinder_interest_stock", group_id=to_group_id, ticker__in=tickers)

        # 3. 해당 종목이 이동하는 그룹에 있으면 예외 처리
        # 3-1. 해당 종목이 이동하는 그룹에 있으면 제외
        set_from_group_tickers = {ticker.ticker for ticker in from_group_tickers}
        set_to_group_tickers = {ticker.ticker for ticker in to_group_tickers}
        move_tickers = set_from_group_tickers - set_to_group_tickers

        # 4. 이동할 그룹의 현재 최대 order 값 조회
        query = "SELECT MAX(`order`) as max_order FROM alphafinder_interest_stock WHERE group_id = :group_id"
        result = self.db._execute(text(query), {"group_id": to_group_id})
        max_order_row = result.fetchone()
        next_order = (max_order_row[0] if max_order_row and max_order_row[0] is not None else 0) + 1

        # 5. 이동하려는 종목들을 From 그룹에서 제거
        self.db._delete(table="alphafinder_interest_stock", group_id=from_group_id, ticker__in=tickers)

        # 6. 이동하려는 종목들을 To 그룹에 추가 (order 값 설정)
        insert_data = [
            {"group_id": to_group_id, "ticker": ticker, "order": next_order + idx}
            for idx, ticker in enumerate(move_tickers)
        ]
        self.db._insert(table="alphafinder_interest_stock", sets=insert_data)
        return True

    def get_interest_price(self, tickers: List[str], group_id: int, lang: TranslateCountry = TranslateCountry.KO):
        if lang == TranslateCountry.KO:
            name_column = "kr_name"
        else:
            name_column = "en_name"
        print(f"tickers : {tickers}")

        ticker_price_data = self.data_db._select(
            table="stock_trend",
            columns=["ctry", "ticker", name_column, "current_price", "change_rt"],
            ticker__in=tickers,
        )
        ticker_price_data = [
            {
                "ctry": row.ctry,
                "ticker": row.ticker,
                "name": self.get_name(row, lang),
                "current_price": row.current_price,
                "change_rt": row.change_rt,
            }
            for row in ticker_price_data
        ]
        print(f"ticker_price_data : {ticker_price_data}")
        # 순서 정렬
        interest_order_data = self.db._select(
            table="alphafinder_interest_stock", columns=["ticker", "order"], group_id=group_id, ticker__in=tickers
        )
        interest_order_data = {row.ticker: row.order for row in interest_order_data}
        print(f"interest_order_data : {interest_order_data}")

        # interest_order_data가 비어있는 경우 원래 순서대로 반환
        if not interest_order_data:
            return ticker_price_data

        ticker_price_data = sorted(ticker_price_data, key=lambda x: interest_order_data[x["ticker"]])
        return ticker_price_data


def get_interest_service() -> InterestService:
    return InterestService()
