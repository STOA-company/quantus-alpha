"""
데이터베이스를 통한 예외 알림 핸들러
"""

import json
import sqlite3
import traceback
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

from .base import BaseHandler

try:
    from sqlalchemy import create_engine, Table, Column, Integer, String, Text, DateTime, MetaData, JSON  # noqa

    SQLALCHEMY_AVAILABLE = True
except ImportError:  # noqa: E722
    SQLALCHEMY_AVAILABLE = False


class DatabaseHandler(BaseHandler):
    """
    데이터베이스에 예외 정보를 저장하는 핸들러

    SQLAlchemy가 설치되어 있으면 사용하고, 그렇지 않으면 sqlite3을 사용합니다.
    """

    def __init__(self, db_url: Optional[str] = None, table_name: str = "exceptions", **kwargs):
        """
        데이터베이스 핸들러 초기화

        Args:
            db_url: 데이터베이스 URL (None일 경우 설정에서 가져옴)
            table_name: 예외를 저장할 테이블 이름
            **kwargs: 추가 설정
        """
        super().__init__(**kwargs)
        self.db_url = db_url or self.config.get("db_url", "sqlite:///exceptions.db")
        self.table_name = table_name

        # SQLAlchemy 사용 여부 결정
        self.use_sqlalchemy = SQLALCHEMY_AVAILABLE and not self.db_url.startswith("sqlite://")

        # 데이터베이스/테이블 초기화
        self._initialize_db()

    def _initialize_db(self) -> None:
        """
        데이터베이스와 테이블 초기화
        """
        if self.use_sqlalchemy:
            try:
                self._initialize_sqlalchemy()
            except Exception as e:
                print(f"Error initializing SQLAlchemy, falling back to sqlite3: {e}")
                self.use_sqlalchemy = False
                self._initialize_sqlite()
        else:
            self._initialize_sqlite()

    def _initialize_sqlalchemy(self) -> None:
        """
        SQLAlchemy를 사용하여 데이터베이스 초기화
        """
        self.engine = create_engine(self.db_url)
        self.metadata = MetaData()

        # 예외 테이블 정의
        self.exceptions_table = Table(
            self.table_name,
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("type", String(128), nullable=False),
            Column("message", Text, nullable=False),
            Column("module", String(128)),
            Column("timestamp", DateTime, nullable=False),
            Column("environment", String(32), nullable=False),
            Column("app_name", String(64), nullable=False),
            Column("hostname", String(128)),
            Column("traceback", Text),
            Column("context", Text),  # JSON 저장
            Column("stack", Text),  # JSON 저장
        )

        # 테이블이 없으면 생성
        self.metadata.create_all(self.engine)

    def _initialize_sqlite(self) -> None:
        """
        SQLite3을 사용하여 데이터베이스 초기화
        """
        # SQL URL에서 파일 경로 추출
        if self.db_url.startswith("sqlite:///"):
            path = self.db_url[10:]
        else:
            path = ":memory:"

        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

        # 테이블 생성
        cursor = self.conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                message TEXT NOT NULL,
                module TEXT,
                timestamp TEXT NOT NULL,
                environment TEXT NOT NULL,
                app_name TEXT NOT NULL,
                hostname TEXT,
                traceback TEXT,
                context TEXT,
                stack TEXT
            )
        """)
        self.conn.commit()
        cursor.close()

    def emit(self, exc_info: Tuple, **context) -> None:
        """
        예외 정보를 데이터베이스에 저장

        Args:
            exc_info: 예외 정보 튜플 (exc_type, exc_value, exc_traceback)
            **context: 추가 컨텍스트 정보
        """
        if not self.should_notify(context.get("level", self.level)):
            return

        # 예외 정보 포맷팅
        exception_data = self.format_exception(exc_info, **context)

        try:
            if self.use_sqlalchemy:
                self._save_with_sqlalchemy(exception_data)
            else:
                self._save_with_sqlite(exception_data)
        except Exception as e:
            print(f"Error saving exception to database: {e}")
            traceback.print_exc()

    def _save_with_sqlalchemy(self, exception_data: Dict[str, Any]) -> None:
        """
        SQLAlchemy를 사용하여 예외 정보 저장

        Args:
            exception_data: 포맷팅된 예외 정보
        """
        # 데이터 준비
        timestamp = datetime.fromisoformat(exception_data["timestamp"])

        # JSON 직렬화
        context_json = json.dumps(exception_data.get("context", {})) if exception_data.get("context") else None
        stack_json = json.dumps(exception_data.get("stack", [])) if exception_data.get("stack") else None

        # 데이터 삽입
        with self.engine.connect() as conn:
            conn.execute(
                self.exceptions_table.insert().values(
                    type=exception_data["type"],
                    message=exception_data["message"],
                    module=exception_data.get("module"),
                    timestamp=timestamp,
                    environment=exception_data["environment"],
                    app_name=exception_data["app_name"],
                    hostname=exception_data.get("hostname"),
                    traceback=exception_data.get("traceback"),
                    context=context_json,
                    stack=stack_json,
                )
            )

    def _save_with_sqlite(self, exception_data: Dict[str, Any]) -> None:
        """
        SQLite3을 사용하여 예외 정보 저장

        Args:
            exception_data: 포맷팅된 예외 정보
        """
        # JSON 직렬화
        context_json = json.dumps(exception_data.get("context", {})) if exception_data.get("context") else None
        stack_json = json.dumps(exception_data.get("stack", [])) if exception_data.get("stack") else None

        # 데이터 삽입
        cursor = self.conn.cursor()
        cursor.execute(
            f"INSERT INTO {self.table_name} "
            f"(type, message, module, timestamp, environment, app_name, hostname, traceback, context, stack) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                exception_data["type"],
                exception_data["message"],
                exception_data.get("module"),
                exception_data["timestamp"],
                exception_data["environment"],
                exception_data["app_name"],
                exception_data.get("hostname"),
                exception_data.get("traceback"),
                context_json,
                stack_json,
            ),
        )
        self.conn.commit()
        cursor.close()

    def close(self) -> None:
        """
        핸들러 리소스 정리
        """
        if not self.use_sqlalchemy and hasattr(self, "conn"):
            self.conn.close()
