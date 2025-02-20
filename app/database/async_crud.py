from dataclasses import asdict
from typing import Optional, Any, List, Dict, AsyncGenerator
from sqlalchemy import MetaData, select, insert, update, desc, asc, or_, and_, Table, bindparam
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from contextlib import asynccontextmanager
import logging
import asyncio
from app.core.config import get_database_config
from app.database.conn import db
from .crud import JoinInfo, ALLOWED_AGGREGATE_FUNCTIONS


logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Base exception class for database operations"""

    pass


class AsyncDatabase:
    def __init__(self):
        c = get_database_config()
        conf_dict = asdict(c)
        self.init_db(conf_dict)

    def init_db(self, conf_dict: dict):
        """Initialize database connection and metadata"""
        db.init_db(**conf_dict)
        self.init_conn()
        self.init_meta()

    def init_conn(self):
        """Initialize async engine"""
        self.engine = db.async_engine

    def init_meta(self):
        """Initialize metadata with synchronous engine for reflection"""
        self.meta_data = MetaData()
        self.meta_data.reflect(bind=db.engine)

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[AsyncSession, None]:
        """Transaction context manager for handling multiple queries in a single transaction"""
        async with db.async_session() as session:
            async with session.begin():
                try:
                    yield session
                except SQLAlchemyError as e:
                    await session.rollback()
                    logger.error(f"Transaction failed: {str(e)}")
                    raise DatabaseError(f"Transaction failed: {str(e)}") from e

    def get_table(self, table_name: str) -> Table:
        """Get table object from metadata"""
        try:
            return self.meta_data.tables[table_name]
        except KeyError:
            raise DatabaseError(f"Table '{table_name}' not found in database")

    def get_condition(self, table: Table, **kwargs) -> list:
        """Build WHERE conditions for queries"""
        try:
            cond = []
            for key, val in kwargs.items():
                if key == "or__":
                    or_cond = []
                    for sub_cond in val:
                        key, val = next(iter(sub_cond.items()))
                        key = key.split("__")
                        col = getattr(table.columns, key[0])

                        if len(key) == 1:
                            or_cond.append(col == val)
                        elif len(key) == 2:
                            if key[1] == "not":
                                or_cond.append(col != val)
                            elif key[1] == "gt":
                                or_cond.append(col > val)
                            elif key[1] == "gte":
                                or_cond.append(col >= val)
                            elif key[1] == "lt":
                                or_cond.append(col < val)
                            elif key[1] == "lte":
                                or_cond.append(col <= val)
                            elif key[1] == "in":
                                or_cond.append(col.in_(val))
                            elif key[1] == "notin":
                                or_cond.append(~col.in_(val))
                            elif key[1] == "like":
                                or_cond.append(col.like(val))
                            else:
                                raise DatabaseError(f"Unknown operator: {key[1]}")

                    if or_cond:
                        cond.append(or_(*or_cond))
                    continue

                key = key.split("__")
                col = getattr(table.columns, key[0])

                if len(key) == 1:
                    cond.append(col == val)
                elif len(key) == 2:
                    if key[1] == "not":
                        cond.append(col != val)
                    elif key[1] == "gt":
                        cond.append(col > val)
                    elif key[1] == "gte":
                        cond.append(col >= val)
                    elif key[1] == "lt":
                        cond.append(col < val)
                    elif key[1] == "lte":
                        cond.append(col <= val)
                    elif key[1] == "in":
                        cond.append(col.in_(val))
                    elif key[1] == "notin":
                        cond.append(~col.in_(val))
                    elif key[1] == "like":
                        cond.append(col.like(val))
                    else:
                        raise DatabaseError(f"Unknown operator: {key[1]}")

            return cond
        except Exception as e:
            raise DatabaseError(f"Error building conditions: {str(e)}") from e

    async def _join(self, join_info: JoinInfo) -> Table:
        """Create JOIN conditions"""
        try:
            primary_table = self.get_table(join_info.primary_table)
            secondary_table = self.get_table(join_info.secondary_table)

            primary_col = getattr(primary_table.columns, join_info.primary_column)
            secondary_col = getattr(secondary_table.columns, join_info.secondary_column)

            conds = [primary_col == secondary_col]
            if join_info.secondary_condition:
                conds += self.get_condition(secondary_table, **join_info.secondary_condition)

            if join_info.is_outer:
                return primary_table.outerjoin(secondary_table, and_(*conds))
            return primary_table.join(secondary_table, and_(*conds))
        except Exception as e:
            raise DatabaseError(f"Error creating join: {str(e)}") from e

    async def _select(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        order: Optional[str] = None,
        ascending: bool = False,
        join_info: Optional[JoinInfo] = None,
        distinct: bool = False,
        group_by: Optional[List[str]] = None,
        aggregates: Optional[Dict[str, tuple]] = None,
        limit: int = 0,
        offset: int = 0,
        session: Optional[AsyncSession] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """Execute SELECT query"""
        try:
            table_obj = self.get_table(table)
            cols = self._prepare_columns(table_obj, columns, join_info, aggregates)
            stmt = self._build_select_query(
                table_obj, cols, distinct, kwargs, join_info, group_by, order, ascending, limit, offset
            )

            if session is None:
                async with db.async_session() as session:
                    result = await session.execute(stmt)
                    return result.mappings().all()
            else:
                result = await session.execute(stmt)
                return result.mappings().all()

        except Exception as e:
            raise DatabaseError(f"Error in select operation: {str(e)}") from e

    async def _insert(
        self, table: str, values: Dict[str, Any] | List[Dict[str, Any]], session: Optional[AsyncSession] = None
    ) -> Any:
        """Execute INSERT query"""
        try:
            table_obj = self.get_table(table)
            stmt = insert(table_obj).values(values)

            if session is None:
                async with db.async_session() as session:
                    async with session.begin():
                        result = await session.execute(stmt)
                        await session.commit()
                        return result
            else:
                result = await session.execute(stmt)
                return result

        except IntegrityError as e:
            raise DatabaseError(f"Integrity error in insert operation: {str(e)}") from e
        except Exception as e:
            raise DatabaseError(f"Error in insert operation: {str(e)}") from e

    async def _bulk_update(
        self,
        table: str,
        data: List[Dict[str, Any]],
        key_column: str,
        chunk_size: int = 1000,
        session: Optional[AsyncSession] = None,
    ) -> None:
        """Execute bulk UPDATE query with rate limiting"""
        if not data:
            return

        try:
            table_obj = self.get_table(table)
            if key_column not in table_obj.columns:
                raise DatabaseError(f"Key column '{key_column}' not found in table '{table}'")

            session_context = session or db.async_session()
            async with session_context as session:
                async with session.begin():
                    for i in range(0, len(data), chunk_size):
                        chunk = data[i : i + chunk_size]
                        stmt = (
                            update(table_obj)
                            .where(getattr(table_obj.columns, key_column) == bindparam(f"_{key_column}"))
                            .values({col: bindparam(col) for col in chunk[0].keys() if col != key_column})
                        )

                        await session.execute(stmt, [{"_" + key_column: item[key_column], **item} for item in chunk])

                        logger.info(f"Processed chunk {i//chunk_size + 1}, {len(chunk)} records")
                        await asyncio.sleep(1)  # Rate limiting

                    if not session:
                        await session.commit()

        except Exception as e:
            raise DatabaseError(f"Error in bulk update operation: {str(e)}") from e

    async def check_connection(self) -> bool:
        """Check database connection status"""
        try:
            async with db.async_session() as session:
                await session.execute(select(1))
            return True
        except Exception as e:
            logger.error(f"Database connection check failed: {str(e)}")
            return False

    def _prepare_columns(
        self,
        table: Table,
        columns: Optional[List[str]],
        join_info: Optional[JoinInfo],
        aggregates: Optional[Dict[str, tuple]],
    ) -> List:
        """Prepare column list for SELECT queries"""
        if not columns:
            return [table]

        cols = []
        # Add columns from main table
        for col in columns:
            if not join_info or col not in (join_info.columns or []):
                cols.append(getattr(table.columns, col))

        # Add columns from joined table
        if join_info and join_info.columns:
            join_table = self.get_table(join_info.secondary_table)
            for col in join_info.columns:
                if col in columns:
                    cols.append(getattr(join_table.columns, col))

        # Add aggregate columns
        if aggregates:
            for alias, (col_name, func_name) in aggregates.items():
                if func_name not in ALLOWED_AGGREGATE_FUNCTIONS:
                    raise DatabaseError(f"Invalid aggregate function: {func_name}")
                column = getattr(table.columns, col_name)
                agg_func = ALLOWED_AGGREGATE_FUNCTIONS[func_name]
                cols.append(agg_func(column).label(alias))

        return cols

    def _build_select_query(
        self,
        table: Table,
        columns: List,
        distinct: bool,
        conditions: Dict,
        join_info: Optional[JoinInfo],
        group_by: Optional[List[str]],
        order: Optional[str],
        ascending: bool,
        limit: int,
        offset: int,
    ):
        """Build SELECT query with all options"""
        stmt = select(*columns)
        if distinct:
            stmt = stmt.distinct()

        # Add conditions
        if conditions:
            stmt = stmt.where(*self.get_condition(table, **conditions))

        # Add join
        if join_info:
            join_condition = self._join(join_info)
            stmt = stmt.select_from(join_condition)

        # Add group by
        if group_by:
            group_cols = [getattr(table.columns, col) for col in group_by]
            stmt = stmt.group_by(*group_cols)

        # Add order by
        if order:
            order_col = getattr(table.columns, order)
            stmt = stmt.order_by(asc(order_col) if ascending else desc(order_col))

        # Add limit and offset
        if limit:
            stmt = stmt.limit(limit)
        if offset:
            stmt = stmt.offset(offset)

        return stmt


# 싱글톤 인스턴스 생성
async_database = AsyncDatabase()
