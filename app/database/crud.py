import logging
from typing import Any, Dict, List, Optional
from dataclasses import asdict, dataclass, field
from sqlalchemy import MetaData
from sqlalchemy import select, insert, update, delete, desc, asc, or_, and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from app.core.config import get_database_config
from app.database.conn import db


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

logger = logging.getLogger(__name__)

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
        # SQLAlchemy 2.0 방식으로 MetaData 초기화
        self.meta_data = MetaData()
        # 엔진에 대해 reflect 수행
        self.meta_data.reflect(bind=self.conn)
        
    def check_connection(self) -> bool:
        """데이터베이스 연결 상태를 확인하는 메서드"""
        try:
            # 간단한 쿼리로 연결 테스트
            with self.conn.connect() as connection:
                connection.execute(select(1))
            return True
        except Exception as e:
            logging.error(f"Database connection check failed: {str(e)}")
            return False
        

    def _execute(self, query, uid=None, *args):
        res = None
        try:
            res = self.conn.execute(query, args)
        except IntegrityError as ie:
            raise ie
            
        return res
            
    def get_condition(self, obj: object, **kwargs) -> list:
        """get condition data

        Args:
            - obj (str): table obj (model)
            - kwargs (dict): {"column__operator": "value"}
                ** operator
                    - 'not': !=
                    - 'gt': >
                    - 'gte': >=
                    - 'lt': <
                    - 'lte': <=
                    - 'in': in_
                    - 'notin': not in
                    
                    * 'or__': OR문 로직
                        'or__': [
                            {key: val},
                            {key: val},
                            ...
                        ]

        Returns:
            list: condition data
        """
        cond = []
        for key, val in kwargs.items():
            if key == "or__": # OR condition key
                or_cond = []
                for sub_cond in val:
                    key, val = list(sub_cond.keys())[0], list(sub_cond.values())[0]
                    key = key.split("__")
                    col = getattr(obj.columns, key[0])
                    if len(key) == 1: or_cond.append((col == val))
                    elif len(key) == 2 and key[1] == 'not': or_cond.append((col != val))
                    elif len(key) == 2 and key[1] == 'gt': or_cond.append((col > val))
                    elif len(key) == 2 and key[1] == 'gte': or_cond.append((col >= val))
                    elif len(key) == 2 and key[1] == 'lt': or_cond.append((col < val))
                    elif len(key) == 2 and key[1] == 'lte': or_cond.append((col <= val))
                    elif len(key) == 2 and key[1] == 'in': or_cond.append((col.in_(val)))
                    elif len(key) == 2 and key[1] == 'notin': or_cond.append(~(col.in_(val)))  
                if or_cond:
                    cond.append(or_(*or_cond))
                continue # OR 조건 처리 후 다음 조건으로 넘어감
            
            key = key.split("__")
            col = getattr(obj.columns, key[0])
            if len(key) == 1: cond.append((col == val))
            elif len(key) == 2 and key[1] == 'not': cond.append((col != val))
            elif len(key) == 2 and key[1] == 'gt': cond.append((col > val))
            elif len(key) == 2 and key[1] == 'gte': cond.append((col >= val))
            elif len(key) == 2 and key[1] == 'lt': cond.append((col < val))
            elif len(key) == 2 and key[1] == 'lte': cond.append((col <= val))
            elif len(key) == 2 and key[1] == 'in': cond.append((col.in_(val)))
            elif len(key) == 2 and key[1] == 'notin': cond.append(~(col.in_(val)))  

        return cond

    def get_sets(self, obj, sets) -> dict:
        
        _sets = {}
        for key, val in sets.items():
            keys = key.split("__")
            col = getattr(obj.columns, keys[0])
            if len(keys) == 1: _sets[col] = val
            elif len(keys) == 2 and keys[1] == "inc": _sets[col] = col + val
        return _sets
        
    def _update(self, table: str, sets: dict, **kwargs):
        if not kwargs:
            raise Exception("Conditional statements (kwargs) are required in update queries.")
        
        obj = self.meta_data.tables[table]
        cond = self.get_condition(obj, **kwargs)
        _sets = self.get_sets(obj, sets)
        stmt = (
                update(obj).
                where(*cond).
                values(_sets)
            )
        return self._execute(stmt, uid=kwargs.get("uid"))
    
    def _delete(self, table: str, **kwargs):
        if not kwargs:
            raise Exception("Conditional statements (kwargs) are required in delete queries.")
        
        obj = self.meta_data.tables[table]
        cond = self.get_condition(obj, **kwargs)
        stmt = (
                delete(obj).
                where(*cond)
            )
        return self._execute(stmt, uid=kwargs.get("uid"))

    def _insert(self, table: str, sets: dict | list):
        
        obj = self.meta_data.tables[table]
        if isinstance(sets, dict):
            uid = sets.get("uid")
            _sets = self.get_sets(obj, sets)
        elif isinstance(sets, list):
            _sets = [self.get_sets(obj, _set) for _set in sets]
            uid = None
        else:
            return

        stmt = (
                insert(obj).
                values(_sets)
            )
        
        return self._execute(stmt, uid=uid)
        
    def _select(
        self, 
        table: str, 
        columns: list | None = None, 
        order: str | None = None , 
        ascending: bool = False, 
        join_info: JoinInfo | None = None, 
        limit: int = 0,
        **kwargs):
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
        stm = select(*cols).where(*cond)
        if join_info:
            join_condition = self._join(join_info)
            stm = stm.select_from(join_condition)
        
        if order:
            orders = [order]
            order_cols = list(map(lambda x: getattr(obj.columns, x), orders))
            if len(order_cols) == 1:
                order_col = order_cols[0]
                if ascending:
                    # 오름차순
                    stm = stm.order_by(asc(order_col))
                else:
                    # 내림차순
                    stm = stm.order_by(desc(order_col))
        if limit:
            stm = stm.limit(limit)
        result = self.conn.execute(stm).fetchall()

        return result
    
    def _join(self, join_info: JoinInfo):
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
    
database = Database()