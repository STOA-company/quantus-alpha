import asyncio
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union

from app.database.crud import database_service
from app.modules.screener.stock.schemas import MarketEnum, SortInfo
from app.modules.screener.etf.enum import ETFMarketEnum
from app.enum.type import StockType
from app.models.models_factors import CategoryEnum
from app.modules.screener.utils import screener_utils
from app.common.constants import (
    FACTOR_MAP,
    REVERSE_FACTOR_MAP,
)
from app.core.exception.base import CustomException
from app.core.logging.config import get_logger

logger = get_logger(__name__)


class BaseScreenerService(ABC):
    """기본 스크리너 서비스 추상 클래스"""

    MAX_GROUPS = 10  # 최대 그룹 수 제한

    def __init__(self):
        self.database = database_service
        self.lang = "kr"

    def get_groups(self, user_id: str, type: Optional[StockType] = StockType.STOCK) -> List[Dict]:
        """
        사용자의 그룹 목록 조회
        """
        try:
            groups = self.database._select(
                table="screener_groups", user_id=user_id, order="order", ascending=True, type=type
            )
            return [
                {
                    "id": group.id,
                    "name": group.name,
                    "type": group.type.lower(),
                }
                for group in groups
            ]
        except Exception as e:
            logger.error(f"Error in get_groups: {e}")
            raise e

    def get_group_filters(self, group_id: int) -> Dict:
        """
        그룹 필터 조회
        """
        try:
            group = self.database._select(table="screener_groups", id=group_id)
            stock_filters = self.database._select(table="screener_stock_filters", group_id=group_id)
            custom_factor_filters = self.database._select(
                table="screener_factor_filters", group_id=group_id, category=CategoryEnum.CUSTOM
            )
            custom_factor_filters = sorted(custom_factor_filters, key=lambda x: x.order)
            has_custom = len(custom_factor_filters) > 0

            return {
                "name": group[0].name,
                "stock_filters": [
                    {
                        "factor": FACTOR_MAP[stock_filter.factor],
                        "value": stock_filter.value if stock_filter.value else None,
                        "above": stock_filter.above if stock_filter.above else None,
                        "below": stock_filter.below if stock_filter.below else None,
                    }
                    for stock_filter in stock_filters
                ],
                "custom_factor_filters": [FACTOR_MAP[factor_filter.factor] for factor_filter in custom_factor_filters],
                "has_custom": has_custom,
            }
        except Exception as e:
            logger.error(f"Error in get_group_filters: {e}")
            raise e

    def delete_group(self, group_id: int) -> bool:
        """
        그룹 삭제
        """
        try:
            self.database._delete(table="screener_groups", id=group_id)  # CASCADE
            return True
        except Exception as e:
            logger.error(f"Error in delete_group: {e}")
            raise e

    def reorder_groups(self, groups: List[int]) -> bool:
        """
        그룹 순서 변경
        """
        try:
            # 각 그룹 ID에 대해 새로운 순서를 포함하는 데이터 리스트 생성
            update_data = [{"id": group_id, "order": index + 1} for index, group_id in enumerate(groups)]

            # bulk update 실행
            self.database._bulk_update(table="screener_groups", data=update_data, key_column="id")
            return True
        except Exception as e:
            logger.error(f"Error in reorder_groups: {e}")
            raise e

    async def reorder_factor_filters(
        self, group_id: int, category: CategoryEnum = CategoryEnum.CUSTOM, factor_filters: List[str] = []
    ) -> bool:
        """
        팩터 필터 순서 변경
        """
        try:
            self.database._delete(table="screener_factor_filters", group_id=group_id, category=category)
            insert_tasks = []
            for idx, factor in enumerate(factor_filters):
                insert_tasks.append(
                    self.database.insert_wrapper(
                        table="screener_factor_filters",
                        sets={
                            "group_id": group_id,
                            "factor": REVERSE_FACTOR_MAP[factor],
                            "order": idx + 1,
                            "category": category,
                        },
                    )
                )
            await asyncio.gather(*insert_tasks)
            return True
        except Exception as e:
            logger.exception(f"Error in reorder_factor_filters: {e}")
            raise e

    def get_columns(
        self,
        group_id: int = -1,
        category: CategoryEnum = CategoryEnum.TECHNICAL,
        type: Optional[StockType] = StockType.STOCK,
    ) -> List[str]:
        """
        컬럼 목록 조회
        """
        try:
            if group_id == -1:
                default_columns = screener_utils.get_default_columns(category=category, type=type)
                result = [FACTOR_MAP[column] for column in default_columns]
                if category == CategoryEnum.DIVIDEND:
                    if "총 수수료" in result:  # 항목이 존재하는지 확인 후 제거
                        result.remove("총 수수료")
                return result

            factor_filters = self.database._select(
                table="screener_factor_filters", columns=["factor", "order"], group_id=group_id, category=category
            )
            factor_filters = sorted(factor_filters, key=lambda x: x.order)

            return [FACTOR_MAP[factor_filter.factor] for factor_filter in factor_filters]

        except Exception as e:
            logger.error(f"Error in get_columns: {e}")
            raise e

    def update_group_name(self, group_id: int, name: str) -> str:
        """
        그룹 이름 변경
        """
        try:
            existing_groups = self.database._select(table="screener_groups", name=name)
            if existing_groups and any(group.id != group_id for group in existing_groups):
                raise CustomException(status_code=409, message="Group name already exists for this type")

            self.database._update(table="screener_groups", id=group_id, sets={"name": name})
            updated_group_name = self.database._select(table="screener_groups", id=group_id)[0].name
            if name == updated_group_name:
                return updated_group_name
            else:
                raise CustomException(status_code=500, message="Failed to update group name")
        except Exception as e:
            logger.error(f"Error in update_group_name: {e}")
            raise e

    def validate_group(self, group_ids: List[int]) -> bool:
        """
        그룹 유효성 검사
        """
        if len(group_ids) > self.MAX_GROUPS:
            raise CustomException(status_code=400, detail="Groups is too long")
        if len(set(group_ids)) != len(group_ids):
            raise CustomException(status_code=400, detail="Groups has duplicate values")
        if any(group_id <= 0 for group_id in group_ids):
            raise CustomException(status_code=400, detail="Groups has negative values")

    def get_group_length(self, user_id: int) -> int:
        """
        그룹 수 조회
        """
        groups = self.database._select(table="screener_groups", columns=["id"], user_id=user_id)
        return len(groups)

    def check_owner(self, group_id: Union[int, List[int]], user_id: int) -> bool:
        """
        그룹 소유자 확인
        """
        if isinstance(group_id, int):
            groups = self.database._select(table="screener_groups", columns=["user_id"], id=group_id)
            group_user_id = int(groups[0].user_id)
            return group_user_id == user_id

        else:
            groups = self.database._select(table="screener_groups", columns=["user_id"], id__in=group_id)
            if len(set([group.user_id for group in groups])) > 1:
                return False
            group_user_id = int(groups[0].user_id)
            return group_user_id == user_id

    def get_sort_info(self, group_id: int, category: CategoryEnum) -> SortInfo:
        """
        정렬 정보 조회
        """
        sort_infos = self.database._select(table="screener_sort_infos", group_id=group_id, category=category)
        if sort_infos:
            return SortInfo(sort_by=FACTOR_MAP[sort_infos[0].sort_by], ascending=sort_infos[0].ascending)
        else:
            return SortInfo(sort_by="스코어", ascending=False)

    async def create_default_factor_filters(self, group_id: int, type: StockType) -> bool:
        """
        기본 팩터 필터 생성
        """
        try:
            technical = screener_utils.get_default_columns(category=CategoryEnum.TECHNICAL, type=type)
            fundamental = screener_utils.get_default_columns(category=CategoryEnum.FUNDAMENTAL, type=type)
            valuation = screener_utils.get_default_columns(category=CategoryEnum.VALUATION, type=type)
            dividend = screener_utils.get_default_columns(category=CategoryEnum.DIVIDEND, type=type)

            insert_tasks = []

            if type == StockType.STOCK:
                for idx, factor in enumerate(technical):
                    insert_tasks.append(
                        self.database.insert_wrapper(
                            table="screener_factor_filters",
                            sets={
                                "group_id": group_id,
                                "factor": factor,
                                "order": idx + 1,
                                "category": CategoryEnum.TECHNICAL,
                            },
                        )
                    )
                for idx, factor in enumerate(fundamental):
                    insert_tasks.append(
                        self.database.insert_wrapper(
                            table="screener_factor_filters",
                            sets={
                                "group_id": group_id,
                                "factor": factor,
                                "order": idx + 1,
                                "category": CategoryEnum.FUNDAMENTAL,
                            },
                        )
                    )

                for idx, factor in enumerate(valuation):
                    insert_tasks.append(
                        self.database.insert_wrapper(
                            table="screener_factor_filters",
                            sets={
                                "group_id": group_id,
                                "factor": factor,
                                "order": idx + 1,
                                "category": CategoryEnum.VALUATION,
                            },
                        )
                    )

                insert_tasks.append(
                    self.database.insert_wrapper(
                        table="screener_sort_infos",
                        sets={
                            "group_id": group_id,
                            "category": CategoryEnum.TECHNICAL,
                            "sort_by": "score",
                            "ascending": False,
                            "type": StockType.STOCK,
                        },
                    )
                )

                insert_tasks.append(
                    self.database.insert_wrapper(
                        table="screener_sort_infos",
                        sets={
                            "group_id": group_id,
                            "category": CategoryEnum.FUNDAMENTAL,
                            "sort_by": "score",
                            "ascending": False,
                            "type": StockType.STOCK,
                        },
                    )
                )
                insert_tasks.append(
                    self.database.insert_wrapper(
                        table="screener_sort_infos",
                        sets={
                            "group_id": group_id,
                            "category": CategoryEnum.VALUATION,
                            "sort_by": "score",
                            "ascending": False,
                            "type": StockType.STOCK,
                        },
                    )
                )

            elif type == StockType.ETF:
                for idx, factor in enumerate(technical):
                    insert_tasks.append(
                        self.database.insert_wrapper(
                            table="screener_factor_filters",
                            sets={
                                "group_id": group_id,
                                "factor": factor,
                                "order": idx + 1,
                                "category": CategoryEnum.TECHNICAL,
                            },
                        )
                    )
                for idx, factor in enumerate(dividend):
                    insert_tasks.append(
                        self.database.insert_wrapper(
                            table="screener_factor_filters",
                            sets={
                                "group_id": group_id,
                                "factor": factor,
                                "order": idx + 1,
                                "category": CategoryEnum.DIVIDEND,
                            },
                        )
                    )

                insert_tasks.append(
                    self.database.insert_wrapper(
                        table="screener_sort_infos",
                        sets={
                            "group_id": group_id,
                            "category": CategoryEnum.TECHNICAL,
                            "sort_by": "score",
                            "ascending": False,
                            "type": StockType.ETF,
                        },
                    )
                )

                insert_tasks.append(
                    self.database.insert_wrapper(
                        table="screener_sort_infos",
                        sets={
                            "group_id": group_id,
                            "category": CategoryEnum.DIVIDEND,
                            "sort_by": "score",
                            "ascending": False,
                            "type": StockType.ETF,
                        },
                    )
                )

            else:
                raise CustomException(status_code=400, message="Invalid type")

            await asyncio.gather(*insert_tasks)

            return True
        except Exception as e:
            logger.error(f"Error in create_default_factor_filters: {e}")
            self.database._delete(table="screener_factor_filters", group_id=group_id)
            raise e

    async def create_group(
        self,
        user_id: int,
        name: str = "기본",
        type: Optional[StockType] = StockType.STOCK,
        market_filter: Optional[Union[MarketEnum, ETFMarketEnum]] = None,
        sector_filter: Optional[List[str]] = [],
        custom_filters: Optional[List[Dict]] = [],
    ) -> bool:
        """
        그룹 생성
        """
        existing_groups = self.database._select(table="screener_groups", user_id=user_id, name=name, type=type)
        if existing_groups:
            raise CustomException(status_code=409, message="Group name already exists for this type")

        try:
            insert_tasks = []

            groups = self.database._select(table="screener_groups", user_id=user_id, order="order", ascending=False)
            if groups:
                order = groups[0].order + 1
            else:
                order = 1

            self.database._insert(
                table="screener_groups", sets={"user_id": user_id, "name": name, "order": order, "type": type}
            )

            group_id = self.database._select(table="screener_groups", user_id=user_id, name=name, type=type)[0].id

            await self.create_default_factor_filters(group_id=group_id, type=type)

            if group_id is None:
                raise CustomException(status_code=500, message="Failed to create group")

            # 종목 필터
            if market_filter:
                insert_tasks.append(
                    self.database.insert_wrapper(
                        table="screener_stock_filters",
                        sets={"group_id": group_id, "factor": "market", "value": market_filter},
                    )
                )

            if sector_filter:
                for sector in sector_filter:
                    insert_tasks.append(
                        self.database.insert_wrapper(
                            table="screener_stock_filters",
                            sets={"group_id": group_id, "factor": "sector", "value": sector},
                        )
                    )

            if custom_filters:
                for condition in custom_filters:
                    insert_tasks.append(
                        self.database.insert_wrapper(
                            table="screener_stock_filters",
                            sets={
                                "group_id": group_id,
                                "factor": REVERSE_FACTOR_MAP[condition.factor],
                                "above": condition.above,
                                "below": condition.below,
                            },
                        )
                    )

            await asyncio.gather(*insert_tasks)

            return True

        except Exception as e:
            if hasattr(e, "orig") and "1062" in str(getattr(e, "orig", "")):
                raise CustomException(status_code=409, message="Group name already exists for this type")

            logger.error(f"Error in create_group: {e}")
            raise e

    async def update_group(
        self,
        group_id: int,
        name: Optional[str] = None,
        market_filter: Optional[Union[MarketEnum, ETFMarketEnum]] = None,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
        factor_filters: Optional[Dict[str, List[str]]] = None,
        category: Optional[CategoryEnum] = CategoryEnum.CUSTOM,
        sort_info: Optional[Dict[CategoryEnum, SortInfo]] = None,
        type: Optional[StockType] = StockType.STOCK,
    ) -> bool:
        """
        그룹 업데이트
        """
        try:
            insert_tasks = []

            if name:
                current_group = self.database._select(table="screener_groups", id=group_id)
                if not current_group:
                    raise ValueError(f"Group with id {group_id} not found")

                current_type = current_group[0].type

                existing_groups = self.database._select(
                    table="screener_groups", user_id=current_group[0].user_id, name=name, type=current_type
                )
                if existing_groups and any(group.id != group_id for group in existing_groups):
                    raise CustomException(status_code=409, message="Group name already exists for this type")

                self.database._update(table="screener_groups", id=group_id, sets={"name": name})

            # 종목 필터
            if custom_filters or market_filter or sector_filter:
                self.database._delete(table="screener_stock_filters", group_id=group_id)

            if market_filter:
                insert_tasks.append(
                    self.database.insert_wrapper(
                        table="screener_stock_filters",
                        sets={
                            "group_id": group_id,
                            "factor": "market",
                            "value": market_filter,
                        },
                    )
                )

            if sector_filter:
                for sector in sector_filter:
                    insert_tasks.append(
                        self.database.insert_wrapper(
                            table="screener_stock_filters",
                            sets={
                                "group_id": group_id,
                                "factor": "sector",
                                "value": sector,
                            },
                        )
                    )

            if custom_filters:
                for condition in custom_filters:
                    insert_tasks.append(
                        self.database.insert_wrapper(
                            table="screener_stock_filters",
                            sets={
                                "group_id": group_id,
                                "factor": REVERSE_FACTOR_MAP[condition.factor],
                                "above": condition.above,
                                "below": condition.below,
                            },
                        )
                    )

            # 팩터 필터
            if factor_filters:
                for category, factors in factor_filters.items():
                    await self.reorder_factor_filters(group_id, category, factors)

            if sort_info:
                for category, sort_data in sort_info.items():
                    print(f"category: {category}, sort_data: {sort_data}")
                    self.database._update(
                        table="screener_sort_infos",
                        group_id=group_id,
                        category=category,
                        type=type,
                        sets={"sort_by": REVERSE_FACTOR_MAP[sort_data.sort_by], "ascending": sort_data.ascending},
                    )

            await asyncio.gather(*insert_tasks)

            return True
        except Exception as e:
            if hasattr(e, "orig") and "1062" in str(getattr(e, "orig", "")):
                raise CustomException(status_code=409, message="Group name already exists for this type")

            logger.error(f"Error in update_group: {e}")
            raise e

    @abstractmethod
    def _is_stock(self) -> bool:
        """
        이 서비스가 주식 관련 서비스인지 여부 반환
        """
        pass

    @abstractmethod
    def get_filtered_data(self, **kwargs):
        """
        필터링된 데이터 조회 추상 메서드
        """
        pass
