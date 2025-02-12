from dataclasses import asdict, dataclass, field
import time

from sqlalchemy import MetaData, bindparam
from sqlalchemy import select, insert, update, delete, desc, asc, or_, and_
from sqlalchemy.exc import IntegrityError
from contextlib import contextmanager
import logging
from sqlalchemy import func
from app.core.config import get_database_config
from app.database.conn import db


@dataclass
class JoinInfo:
    primary_table: str
    secondary_table: str
    primary_column: str
    secondary_column: str
    columns: list = field(default_factory=list)
    is_outer: bool = False
    secondary_condition: dict = field(default_factory=dict)


ALLOWED_AGGREGATE_FUNCTIONS = {
    "count": func.count,
    "sum": func.sum,
    "avg": func.avg,
    "min": func.min,
    "max": func.max,
}


class Database:
    def __init__(self):
        c = get_database_config()
        conf_dict = asdict(c)
        self.init_db(conf_dict)

    def init_db(self, conf_dict: dict):
        db.init_db(**conf_dict)
        self.init_conn()
        self.init_meta()

    def init_conn(self):
        self.conn = db.engine

    def init_meta(self):
        self.meta_data = MetaData()
        self.meta_data.reflect(bind=self.conn)

    @contextmanager
    def get_connection(self):
        """컨텍스트 매니저로 connection 관리"""
        with self.conn.connect() as connection:
            try:
                yield connection
                connection.commit()
            except Exception as e:
                connection.rollback()
                raise e

    def check_connection(self) -> bool:
        """데이터베이스 연결 상태를 확인하는 메서드"""
        try:
            with self.get_connection() as connection:
                connection.execute(select(1))
            return True
        except Exception as e:
            logging.error(f"Database connection check failed: {str(e)}")
            return False

    def _execute(self, query, *args):
        """쿼리 실행을 위한 메서드"""
        with self.get_connection() as connection:
            try:
                result = connection.execute(query, *args)
                return result
            except IntegrityError as e:
                logging.error(f"Integrity Error in query execution: {str(e)}")
                raise
            except Exception as e:
                logging.error(f"Error in query execution: {str(e)}")
                raise

    def get_condition(self, obj: object, **kwargs) -> list:
        """조건절 생성 메서드"""
        cond = []
        for key, val in kwargs.items():
            if key == "or__":
                or_cond = []
                for sub_cond in val:
                    key, val = list(sub_cond.keys())[0], list(sub_cond.values())[0]
                    key = key.split("__")
                    col = getattr(obj.columns, key[0])
                    if len(key) == 1:
                        or_cond.append((col == val))
                    elif len(key) == 2 and key[1] == "not":
                        or_cond.append((col != val))
                    elif len(key) == 2 and key[1] == "gt":
                        or_cond.append((col > val))
                    elif len(key) == 2 and key[1] == "gte":
                        or_cond.append((col >= val))
                    elif len(key) == 2 and key[1] == "lt":
                        or_cond.append((col < val))
                    elif len(key) == 2 and key[1] == "lte":
                        or_cond.append((col <= val))
                    elif len(key) == 2 and key[1] == "in":
                        or_cond.append((col.in_(val)))
                    elif len(key) == 2 and key[1] == "notin":
                        or_cond.append(~(col.in_(val)))
                    elif len(key) == 2 and key[1] == "like":
                        or_cond.append(col.like(val))
                if or_cond:
                    cond.append(or_(*or_cond))
                continue

            key = key.split("__")
            col = getattr(obj.columns, key[0])
            if len(key) == 1:
                cond.append((col == val))
            elif len(key) == 2 and key[1] == "not":
                cond.append((col != val))
            elif len(key) == 2 and key[1] == "gt":
                cond.append((col > val))
            elif len(key) == 2 and key[1] == "gte":
                cond.append((col >= val))
            elif len(key) == 2 and key[1] == "lt":
                cond.append((col < val))
            elif len(key) == 2 and key[1] == "lte":
                cond.append((col <= val))
            elif len(key) == 2 and key[1] == "in":
                cond.append((col.in_(val)))
            elif len(key) == 2 and key[1] == "notin":
                cond.append(~(col.in_(val)))
            elif len(key) == 2 and key[1] == "like":
                cond.append(col.like(val))

        return cond

    def get_sets(self, obj, sets) -> dict:
        """SET절 생성 메서드"""
        _sets = {}
        for key, val in sets.items():
            keys = key.split("__")
            col = getattr(obj.columns, keys[0])
            if len(keys) == 1:
                _sets[col] = val
            elif len(keys) == 2 and keys[1] == "inc":
                _sets[col] = col + val
        return _sets

    def _update(self, table: str, sets: dict, **kwargs):
        """UPDATE 쿼리 실행"""
        if not kwargs:
            raise ValueError("Conditional statements (kwargs) are required in update queries.")

        try:
            obj = self.meta_data.tables[table]
            cond = self.get_condition(obj, **kwargs)
            _sets = self.get_sets(obj, sets)
            stmt = update(obj).where(*cond).values(_sets)

            with self.get_connection() as connection:
                result = connection.execute(stmt)
                return result
        except Exception as e:
            logging.error(f"Error in update operation: {str(e)}")
            raise

    def _delete(self, table: str, **kwargs):
        """DELETE 쿼리 실행"""
        if not kwargs:
            raise ValueError("Conditional statements (kwargs) are required in delete queries.")

        try:
            obj = self.meta_data.tables[table]
            cond = self.get_condition(obj, **kwargs)
            stmt = delete(obj).where(*cond)

            with self.get_connection() as connection:
                result = connection.execute(stmt)
                return result
        except Exception as e:
            logging.error(f"Error in delete operation: {str(e)}")
            raise

    def _insert(self, table: str, sets: dict | list):
        """INSERT 쿼리 실행"""
        try:
            obj = self.meta_data.tables[table]
            if isinstance(sets, dict):
                _sets = self.get_sets(obj, sets)
            elif isinstance(sets, list):
                _sets = [self.get_sets(obj, _set) for _set in sets]
            else:
                raise ValueError("Invalid sets parameter type")

            stmt = insert(obj).values(_sets)

            with self.get_connection() as connection:
                result = connection.execute(stmt)
                return result
        except Exception as e:
            logging.error(f"Error in insert operation: {str(e)}")
            raise

    def _select(
        self,
        table: str,
        columns: list | None = None,
        order: str | None = None,
        ascending: bool = False,
        join_info: JoinInfo | None = None,
        distinct: bool = False,
        group_by: list | None = None,
        aggregates: dict | None = None,
        limit: int = 0,
        offset: int = 0,
        **kwargs,
    ):
        """ """

        try:
            obj = self.meta_data.tables[table]

            if columns is None:
                cols = [obj]
            else:
                primary_cols = []
                if join_info:
                    for col in columns:
                        if col not in join_info.columns:
                            try:
                                primary_cols.append(getattr(obj.columns, col))
                            except AttributeError:
                                continue
                else:
                    primary_cols = [getattr(obj.columns, col) for col in columns]
                cols = primary_cols

            if join_info:
                join_table_obj = self.meta_data.tables[join_info.secondary_table]
                join_cols = []
                for col in join_info.columns:
                    if col in columns:
                        join_cols.append(getattr(join_table_obj.columns, col))
                cols.extend(join_cols)

            if aggregates:
                for alias, (col_name, func_name) in aggregates.items():
                    if func_name not in ("count", "sum", "avg", "min", "max"):
                        raise ValueError(
                            f"Invalid aggregate function: {func_name}. "
                            f"Allowed functions are: count, sum, avg, min, max"
                        )

                    if not hasattr(obj.columns, col_name):
                        raise ValueError(f"Invalid column for aggregation: {col_name}")

                    column = getattr(obj.columns, col_name)
                    agg_func = getattr(func, func_name)
                    cols.append(agg_func(column).label(alias))

            cond = self.get_condition(obj, **kwargs)
            stmt = select(*cols)

            if distinct:
                stmt = stmt.distinct()

            stmt = stmt.where(*cond)

            if join_info:
                join_condition = self._join(join_info)
                stmt = stmt.select_from(join_condition)

            if group_by:
                group_cols = [getattr(obj.columns, col) for col in group_by]
                stmt = stmt.group_by(*group_cols)

            if order:
                order_col = getattr(obj.columns, order)
                if ascending:
                    stmt = stmt.order_by(asc(order_col))
                else:
                    stmt = stmt.order_by(desc(order_col))

            if limit:
                stmt = stmt.limit(limit)

            if offset:
                stmt = stmt.offset(offset)

            with self.get_connection() as connection:
                result = connection.execute(stmt)
                return result.fetchall()

        except Exception as e:
            logging.error(f"Error in select operation: {str(e)}")
            raise

    def _join(self, join_info: JoinInfo):
        """JOIN 조건 생성"""
        try:
            primary_obj = self.meta_data.tables[join_info.primary_table]
            secondary_obj = self.meta_data.tables[join_info.secondary_table]

            primary_col = getattr(primary_obj.columns, join_info.primary_column)
            secondary_col = getattr(secondary_obj.columns, join_info.secondary_column)

            conds = [primary_col == secondary_col]
            if join_info.secondary_condition:
                conds += self.get_condition(secondary_obj, **join_info.secondary_condition)

            if join_info.is_outer:
                join_obj = primary_obj.outerjoin(secondary_obj, and_(*conds))
            else:
                join_obj = primary_obj.join(secondary_obj, and_(*conds))

            return join_obj
        except Exception as e:
            logging.error(f"Error in join operation: {str(e)}")
            raise

    def _count(self, table: str, join_info: JoinInfo | None = None, **kwargs) -> int:
        """COUNT 쿼리 실행

        Args:
            table (str): 테이블 이름
            join_info (JoinInfo, optional): 조인 정보. Defaults to None.
            **kwargs: 검색 조건

        Returns:
            int: 조건에 맞는 레코드 수

        Raises:
            Exception: 쿼리 실행 중 오류 발생시
        """
        try:
            obj = self.meta_data.tables[table]
            cond = self.get_condition(obj, **kwargs)

            stmt = select(func.count()).select_from(obj).where(*cond)

            if join_info:
                join_condition = self._join(join_info)
                stmt = stmt.select_from(join_condition)

            with self.get_connection() as connection:
                result = connection.execute(stmt)
                return result.scalar() or 0

        except Exception as e:
            logging.error(f"Error in count operation: {str(e)}")
            raise

    def _bulk_update(self, table: str, data: list[dict], key_column: str, chunk_size: int = 1000):
        """벌크 업데이트를 수행하는 메서드입니다.

        Args:
            table (str): 업데이트할 테이블 이름
            data (list[dict]): 업데이트할 데이터 리스트. 각 딕셔너리는 컬럼명을 키로 가지며, 업데이트할 값을 값으로 가집니다.
            key_column (str): 업데이트 대상을 식별하기 위한 키 컬럼명
            chunk_size (int, optional): 한 번에 처리할 레코드 수. 기본값은 1000입니다.

        Raises:
            ValueError: 다음의 경우에 발생합니다:
                - key_column이 테이블에 존재하지 않는 경우
                - data에 테이블에 존재하지 않는 컬럼이 포함된 경우
            Exception: 데이터베이스 작업 중 오류가 발생한 경우

        Example:
            >>> data = [
            ...     {"ticker": "AAPL", "price": 150.0, "volume": 1000000},
            ...     {"ticker": "GOOGL", "price": 2800.0, "volume": 500000}
            ... ]
            >>> database._bulk_update(
            ...     table="stock_prices",
            ...     data=data,
            ...     key_column="ticker"
            ... )
        """
        try:
            if not data:
                return

            obj = self.meta_data.tables[table]

            if key_column not in obj.columns:
                raise ValueError(f"Key column '{key_column}' does not exist in table '{table}'")

            sample_data = data[0]
            invalid_columns = [col for col in sample_data if col not in obj.columns]
            if invalid_columns:
                raise ValueError(f"Invalid columns found: {invalid_columns}")

            key_col = getattr(obj.columns, key_column)

            for i in range(0, len(data), chunk_size):
                chunk = data[i : i + chunk_size]

                stmt = (
                    update(obj)
                    .where(key_col == bindparam("_old_" + key_column))
                    .values({col_name: bindparam(col_name) for col_name in chunk[0].keys() if col_name in obj.columns})
                )

                update_params = [{"_old_" + key_column: item[key_column], **item} for item in chunk]

                with self.get_connection() as connection:
                    connection.execute(stmt, update_params)

                logging.info(
                    f"Bulk update completed for chunk {i//chunk_size + 1}, " f"processed {len(chunk)} records in {table}"
                )
                time.sleep(1)

        except Exception as e:
            logging.error(f"Error in bulk update operation: {str(e)}")
            raise

    def _bulk_insert(self, table: str, data_list: list[dict], chunk_size: int = 1000):
        """대량의 데이터를 효율적으로 삽입하는 메서드입니다.

        Args:
            table (str): 데이터를 삽입할 테이블 이름
            data_list (list[dict]): 삽입할 데이터 리스트. 각 딕셔너리는 컬럼명을 키로 가지며, 삽입할 값을 값으로 가집니다.
            chunk_size (int, optional): 한 번에 처리할 레코드 수. 기본값은 1000입니다.

        Raises:
            ValueError: data_list가 비어있거나, 테이블에 존재하지 않는 컬럼이 포함된 경우
            Exception: 데이터베이스 작업 중 오류가 발생한 경우

        Example:
            >>> data = [
            ...     {"ticker": "AAPL", "price": 150.0, "volume": 1000000},
            ...     {"ticker": "GOOGL", "price": 2800.0, "volume": 500000}
            ... ]
            >>> database._bulk_insert(
            ...     table="stock_prices",
            ...     data_list=data
            ... )
        """
        try:
            if not data_list:
                raise ValueError("data_list cannot be empty")

            obj = self.meta_data.tables[table]

            sample_data = data_list[0]
            invalid_columns = [col for col in sample_data if col not in obj.columns]
            if invalid_columns:
                raise ValueError(f"Invalid columns found: {invalid_columns}")

            for i in range(0, len(data_list), chunk_size):
                chunk = data_list[i : i + chunk_size]

                processed_chunk = []
                for item in chunk:
                    processed_item = {}
                    for col, val in item.items():
                        if col in obj.columns:
                            processed_item[getattr(obj.columns, col)] = val
                    processed_chunk.append(processed_item)

                stmt = insert(obj).values(chunk)

                with self.get_connection() as connection:
                    connection.execute(stmt)

                logging.info(
                    f"Bulk insert completed for chunk {i//chunk_size + 1}, " f"processed {len(chunk)} records in {table}"
                )
                time.sleep(1)

        except Exception as e:
            logging.error(f"Error in bulk insert operation: {str(e)}")
            raise


database = Database()
