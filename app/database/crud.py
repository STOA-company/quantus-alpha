from dataclasses import asdict, dataclass, field
from sqlalchemy import MetaData
from sqlalchemy import select, insert, update, delete, desc, asc, or_, and_
from sqlalchemy.exc import IntegrityError
from contextlib import contextmanager
import logging
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
        limit: int = 0,
        **kwargs,
    ):
        """SELECT 쿼리 실행"""
        try:
            obj = self.meta_data.tables[table]

            if columns is None:
                cols = [obj]
            else:
                cols = list(map(lambda x: getattr(obj.columns, x), columns))

            if join_info:
                join_table_obj = self.meta_data.tables[join_info.secondary_table]
                join_cols = list(map(lambda x: getattr(join_table_obj.columns, x), join_info.columns))
                cols.extend(join_cols)

            cond = self.get_condition(obj, **kwargs)
            stmt = select(*cols).where(*cond)

            if join_info:
                join_condition = self._join(join_info)
                stmt = stmt.select_from(join_condition)

            if order:
                order_col = getattr(obj.columns, order)
                if ascending:
                    stmt = stmt.order_by(asc(order_col))
                else:
                    stmt = stmt.order_by(desc(order_col))

            if limit:
                stmt = stmt.limit(limit)

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


database = Database()
