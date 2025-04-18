import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field

from sqlalchemy import MetaData, and_, asc, bindparam, delete, desc, func, insert, or_, select, update
from sqlalchemy.exc import IntegrityError

from app.core.config import get_database_config
from app.core.logger import setup_logger
from app.database.conn import db, db_service

logger = setup_logger(__name__)


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


class BaseDatabase:
    def __init__(self, db_connection):
        self.db = db_connection
        c = get_database_config()
        conf_dict = asdict(c)
        self.init_db(conf_dict)

    def init_db(self, conf_dict: dict):
        if not self.db:
            raise ValueError("Database connection must be set")
        self.db.init_db(**conf_dict)
        self.init_conn()
        self.init_meta()

    def init_conn(self):
        self.conn = self.db.engine

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
            logger.error(f"Database connection check failed: {str(e)}")
            return False

    def _execute(self, query, *args):
        """쿼리 실행을 위한 메서드"""
        with self.get_connection() as connection:
            try:
                result = connection.execute(query, *args)
                return result
            except IntegrityError as e:
                logger.error(f"Integrity Error in query execution: {str(e)}")
                raise
            except Exception as e:
                logger.error(f"Error in query execution: {str(e)}")
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
            logger.error(f"Error in update operation: {str(e)}")
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
            logger.error(f"Error in delete operation: {str(e)}")
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
            logger.error(f"Error in insert operation: {str(e)}")
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
                    if func_name not in ALLOWED_AGGREGATE_FUNCTIONS:
                        raise ValueError(
                            f"Invalid aggregate function: {func_name}. "
                            f"Allowed functions are: {', '.join(ALLOWED_AGGREGATE_FUNCTIONS.keys())}"
                        )

                    if not hasattr(obj.columns, col_name):
                        raise ValueError(f"Invalid column for aggregation: {col_name}")

                    column = getattr(obj.columns, col_name)
                    agg_func = ALLOWED_AGGREGATE_FUNCTIONS[func_name]
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
            logger.error(f"Error in select operation: {str(e)}")
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
            logger.error(f"Error in join operation: {str(e)}")
            raise

    def _count(self, table: str, join_info: JoinInfo | None = None, **kwargs) -> int:
        """COUNT 쿼리 실행"""
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
            logger.error(f"Error in count operation: {str(e)}")
            raise

    def _bulk_update(self, table: str, data: list[dict], key_column: str, chunk_size: int = 1000):
        """벌크 업데이트를 수행하는 메서드"""
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

                logger.info(
                    f"Bulk update completed for chunk {i//chunk_size + 1}, " f"processed {len(chunk)} records in {table}"
                )
                time.sleep(1)

        except Exception as e:
            logger.error(f"Error in bulk update operation: {str(e)}")
            raise

    async def insert_wrapper(self, table: str, sets: dict | list):
        """INSERT 쿼리 실행"""
        try:
            return self._insert(table, sets)
        except Exception as e:
            logger.error(f"Error in insert operation: {str(e)}")
            raise

    def _bulk_update_multi_key(self, table: str, data: list[dict], key_columns: list[str], chunk_size: int = 1000):
        """벌크 업데이트를 수행하는 메서드 (복합 키 지원)

        Args:
            table: 업데이트할 테이블 이름
            data: 업데이트할 데이터 리스트 (각 항목은 딕셔너리)
            key_columns: 레코드를 식별하는 키 컬럼 이름 리스트
            chunk_size: 한 번에 처리할 레코드 수 (기본값 1000)
        """
        try:
            if not data:
                return

            obj = self.meta_data.tables[table]

            # 키 컬럼들이 테이블에 존재하는지 확인
            for key_col in key_columns:
                if key_col not in obj.columns:
                    raise ValueError(f"Key column '{key_col}' does not exist in table '{table}'")

            sample_data = data[0]
            invalid_columns = [col for col in sample_data if col not in obj.columns]
            if invalid_columns:
                raise ValueError(f"Invalid columns found: {invalid_columns}")

            # 업데이트할 컬럼 목록 (키 컬럼 제외)
            update_columns = [col for col in sample_data.keys() if col not in key_columns]

            for i in range(0, len(data), chunk_size):
                chunk = data[i : i + chunk_size]

                # WHERE 조건 생성 (복합 키)
                where_conditions = [
                    getattr(obj.columns, key_col) == bindparam("_old_" + key_col) for key_col in key_columns
                ]

                # UPDATE 문 생성
                stmt = (
                    update(obj)
                    .where(and_(*where_conditions))
                    .values({col_name: bindparam(col_name) for col_name in update_columns})
                )

                # 파라미터 생성
                update_params = []
                for item in chunk:
                    param = {"_old_" + key_col: item[key_col] for key_col in key_columns}
                    param.update({col: item[col] for col in update_columns})
                    update_params.append(param)

                with self.get_connection() as connection:
                    connection.execute(stmt, update_params)

                logger.info(
                    f"Bulk update completed for chunk {i//chunk_size + 1}, " f"processed {len(chunk)} records in {table}"
                )
                time.sleep(1)

        except Exception as e:
            logger.error(f"Error in bulk update operation: {str(e)}")
            raise


class Database(BaseDatabase):
    def __init__(self):
        self.db = db
        super().__init__(self.db)


class DatabaseService(BaseDatabase):
    def __init__(self):
        self.db = db_service
        super().__init__(self.db)


database = Database()
database_service = DatabaseService()
