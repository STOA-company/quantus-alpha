from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from sqlalchemy import create_engine, MetaData
from sqlalchemy import select, insert, update, delete, desc, asc, or_, and_
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

Base = declarative_base()


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
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)

    @contextmanager
    def get_session(self):
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_table(self, table_name: str):
        return self.metadata.tables[table_name]

    def get_condition(self, table_obj: Any, **kwargs) -> list:
        """Get condition data for SQL queries"""
        cond = []
        for key, val in kwargs.items():
            if key == "or__":
                or_cond = []
                for sub_cond in val:
                    key, val = list(sub_cond.keys())[0], list(sub_cond.values())[0]
                    key = key.split("__")
                    col = getattr(table_obj.c, key[0])
                    if len(key) == 1:
                        or_cond.append((col == val))
                    elif len(key) == 2:
                        if key[1] == "not":
                            or_cond.append((col != val))
                        elif key[1] == "gt":
                            or_cond.append((col > val))
                        elif key[1] == "gte":
                            or_cond.append((col >= val))
                        elif key[1] == "lt":
                            or_cond.append((col < val))
                        elif key[1] == "lte":
                            or_cond.append((col <= val))
                        elif key[1] == "in":
                            or_cond.append((col.in_(val)))
                        elif key[1] == "notin":
                            or_cond.append(~(col.in_(val)))
                if or_cond:
                    cond.append(or_(*or_cond))
                continue

            key = key.split("__")
            col = getattr(table_obj.c, key[0])
            if len(key) == 1:
                cond.append((col == val))
            elif len(key) == 2:
                if key[1] == "not":
                    cond.append((col != val))
                elif key[1] == "gt":
                    cond.append((col > val))
                elif key[1] == "gte":
                    cond.append((col >= val))
                elif key[1] == "lt":
                    cond.append((col < val))
                elif key[1] == "lte":
                    cond.append((col <= val))
                elif key[1] == "in":
                    cond.append((col.in_(val)))
                elif key[1] == "notin":
                    cond.append(~(col.in_(val)))

        return cond

    def get_sets(self, table_obj: Any, sets: Dict[str, Any]) -> Dict[str, Any]:
        """Get sets data for update queries"""
        _sets = {}
        for key, val in sets.items():
            keys = key.split("__")
            col = getattr(table_obj.c, keys[0])
            if len(keys) == 1:
                _sets[col] = val
            elif len(keys) == 2 and keys[1] == "inc":
                _sets[col] = col + val
        return _sets

    def _join(self, join_info: JoinInfo):
        """Handle table joins"""
        primary_obj = self.get_table(join_info.primary_table)
        secondary_obj = self.get_table(join_info.secondary_table)

        primary_col = getattr(primary_obj.c, join_info.primary_column)
        secondary_col = getattr(secondary_obj.c, join_info.secondary_column)

        conds = [primary_col == secondary_col]
        if join_info.secondary_condition:
            conds += self.get_condition(secondary_obj, **join_info.secondary_condition)

        if join_info.is_outer:
            return primary_obj.outerjoin(secondary_obj, and_(*conds))
        return primary_obj.join(secondary_obj, and_(*conds))

    async def select(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        order: Optional[str] = None,
        ascending: bool = False,
        join_info: Optional[JoinInfo] = None,
        limit: int = 0,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        table_obj = self.get_table(table)

        if columns is None:
            cols = [table_obj]
        else:
            cols = [getattr(table_obj.c, col) for col in columns]

        if join_info:
            join_table_obj = self.get_table(join_info.secondary_table)
            join_cols = [getattr(join_table_obj.c, col) for col in join_info.columns]
            cols.extend(join_cols)

        cond = self.get_condition(table_obj, **kwargs)
        query = select(*cols).where(*cond)

        if join_info:
            join_condition = self._join(join_info)
            query = query.select_from(join_condition)

        if order:
            order_col = getattr(table_obj.c, order)
            query = query.order_by(asc(order_col) if ascending else desc(order_col))

        if limit:
            query = query.limit(limit)

        with self.get_session() as session:
            result = session.execute(query)
            return [dict(row) for row in result]

    async def insert(self, table: str, values: Dict[str, Any] | List[Dict[str, Any]]) -> Any:
        table_obj = self.get_table(table)

        if isinstance(values, dict):
            _sets = self.get_sets(table_obj, values)
        elif isinstance(values, list):
            _sets = [self.get_sets(table_obj, value) for value in values]
        else:
            return None

        query = insert(table_obj).values(_sets)

        with self.get_session() as session:
            try:
                result = session.execute(query)
                return result.inserted_primary_key[0] if isinstance(values, dict) else True
            except IntegrityError as ie:
                raise ie

    async def update(self, table: str, sets: Dict[str, Any], **kwargs) -> int:
        if not kwargs:
            raise Exception("Conditional statements (kwargs) are required in update queries.")

        table_obj = self.get_table(table)
        cond = self.get_condition(table_obj, **kwargs)
        _sets = self.get_sets(table_obj, sets)

        query = update(table_obj).where(*cond).values(_sets)

        with self.get_session() as session:
            try:
                result = session.execute(query)
                return result.rowcount
            except IntegrityError as ie:
                raise ie

    async def delete(self, table: str, **kwargs) -> int:
        if not kwargs:
            raise Exception("Conditional statements (kwargs) are required in delete queries.")

        table_obj = self.get_table(table)
        cond = self.get_condition(table_obj, **kwargs)
        query = delete(table_obj).where(*cond)

        with self.get_session() as session:
            try:
                result = session.execute(query)
                return result.rowcount
            except IntegrityError as ie:
                raise ie
