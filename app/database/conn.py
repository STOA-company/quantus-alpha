from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from app.core.logger import setup_logger

logger = setup_logger(__name__)
import pymysql

pymysql.install_as_MySQLdb()


class BaseSQLAlchemy:
    def __init__(self, app: FastAPI = None, url_key: str = None, **kwargs):
        self._engine = None
        self._async_engine = None
        self._session = None
        self._async_session = None
        self.url_key = url_key  # 하위 클래스에서 설정 (DB_URL 또는 DB_SERVICE_URL)
        if app is not None:
            self.init_app(app=app, **kwargs)

    def init_app(self, app: FastAPI, **kwargs):
        """Initialize app with FastAPI instance"""
        database_url = kwargs.get(self.url_key)
        if not database_url:
            raise ValueError(f"{self.url_key} must be provided")

        # Azure MySQL 체크
        is_azure = 'azure.com' in str(database_url)
        
        # Azure MySQL의 경우 URL에 SSL 파라미터 추가
        if is_azure:
            # URL에 charset이 없으면 추가
            if '?' not in database_url:
                database_url = f"{database_url}?charset=utf8mb4"
            elif 'charset' not in database_url:
                database_url = f"{database_url}&charset=utf8mb4"
        
        async_database_url = database_url.replace("mysql://", "mysql+aiomysql://")
        pool_recycle = kwargs.setdefault("DB_POOL_RECYCLE", 3600)
        pool_size = kwargs.get("DB_POOL_SIZE", 50)  # kwargs에서 직접 가져오기
        max_overflow = kwargs.get("DB_MAX_OVERFLOW", 20)  # kwargs에서 직접 가져오기
        echo = kwargs.setdefault("DB_ECHO", True)

        # 엔진 설정
        engine_kwargs = {
            'echo': echo,
            'pool_recycle': pool_recycle,
            'pool_pre_ping': True,
        }
        
        # Azure MySQL SSL 설정 (pymysql용)
        if is_azure:
            engine_kwargs['connect_args'] = {
                'ssl': {'check_hostname': False, 'verify_mode': 0},
                'ssl_verify_cert': False,
                'charset': 'utf8mb4',
                'connect_timeout': 30,
                'read_timeout': 30,
                'write_timeout': 30
            }
            logger.info("Azure MySQL SSL configuration applied")

        # 동기 엔진 설정
        self._engine = create_engine(
            database_url,
            **engine_kwargs
        )

        self._session = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self._engine,
        )

        # 비동기 엔진 설정
        async_engine_kwargs = {
            'echo': echo,
            'pool_recycle': pool_recycle,
            'pool_pre_ping': True,
            'pool_size': pool_size,
            'max_overflow': max_overflow,
            'pool_timeout': 10,  # 연결 대기 타임아웃 10초
        }
        
        # Azure MySQL SSL 설정 (aiomysql용)
        if is_azure:
            import ssl as ssl_module
            ssl_context = ssl_module.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl_module.CERT_NONE
            
            async_engine_kwargs['connect_args'] = {
                'ssl': ssl_context,
                'charset': 'utf8mb4',
                'connect_timeout': 30,
            }
            logger.info("Azure MySQL async SSL configuration applied")
        
        self._async_engine = create_async_engine(
            async_database_url,
            **async_engine_kwargs
        )

        self._async_session = sessionmaker(
            class_=AsyncSession,
            autocommit=False,
            autoflush=False,
            bind=self._async_engine,
        )

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup
            self._engine.connect()
            async with self._async_engine.connect() as conn:
                await conn.close()
            logger.info("DB connected (both sync and async).")
            yield
            # Shutdown
            self._session.close_all()
            self._engine.dispose()
            await self._async_engine.dispose()
            logger.info("DB disconnected (both sync and async).")

        app.router.lifespan_context = lifespan

    def init_db(self, **kwargs):
        """Initialize DB without FastAPI instance"""
        database_url = kwargs.get(self.url_key)
        if not database_url:
            raise ValueError(f"{self.url_key} must be provided")

        # Azure MySQL 체크
        is_azure = 'azure.com' in str(database_url)
        
        # Azure MySQL의 경우 URL에 SSL 파라미터 추가
        if is_azure:
            # URL에 charset이 없으면 추가
            if '?' not in database_url:
                database_url = f"{database_url}?charset=utf8mb4"
            elif 'charset' not in database_url:
                database_url = f"{database_url}&charset=utf8mb4"
        
        async_database_url = database_url.replace("mysql://", "mysql+aiomysql://")
        pool_recycle = kwargs.setdefault("DB_POOL_RECYCLE", 3600)
        pool_size = kwargs.get("DB_POOL_SIZE", 50)  # kwargs에서 직접 가져오기
        max_overflow = kwargs.get("DB_MAX_OVERFLOW", 20)  # kwargs에서 직접 가져오기
        echo = kwargs.setdefault("DB_ECHO", True)

        # 엔진 설정
        engine_kwargs = {
            'echo': echo,
            'pool_recycle': pool_recycle,
            'pool_pre_ping': True,
        }
        
        # Azure MySQL SSL 설정 (pymysql용)
        if is_azure:
            engine_kwargs['connect_args'] = {
                'ssl': {'check_hostname': False, 'verify_mode': 0},
                'ssl_verify_cert': False,
                'charset': 'utf8mb4',
                'connect_timeout': 30,
                'read_timeout': 30,
                'write_timeout': 30
            }
            logger.info("Azure MySQL SSL configuration applied (init_db)")

        self._engine = create_engine(
            database_url,
            **engine_kwargs
        )

        self._session = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self._engine,
        )

        # 비동기 엔진 설정
        async_engine_kwargs = {
            'echo': echo,
            'pool_recycle': pool_recycle,
            'pool_pre_ping': True,
            'pool_size': pool_size,
            'max_overflow': max_overflow,
            'pool_timeout': 10,  # 연결 대기 타임아웃 10초
        }
        
        # Azure MySQL SSL 설정 (aiomysql용)
        if is_azure:
            import ssl as ssl_module
            ssl_context = ssl_module.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl_module.CERT_NONE
            
            async_engine_kwargs['connect_args'] = {
                'ssl': ssl_context,
                'charset': 'utf8mb4',
                'connect_timeout': 30,
            }
            logger.info("Azure MySQL async SSL configuration applied (init_db)")
        
        self._async_engine = create_async_engine(
            async_database_url,
            **async_engine_kwargs
        )

        self._async_session = sessionmaker(
            class_=AsyncSession,
            autocommit=False,
            autoflush=False,
            bind=self._async_engine,
        )

    def get_db(self):
        """동기 데이터베이스 세션"""
        if self._session is None:
            raise Exception("must be called `init_app` or `init_db`")
        db_session = None
        try:
            db_session = self._session()
            yield db_session
        finally:
            db_session.close()

    async def get_async_db(self):
        """비동기 데이터베이스 세션"""
        if self._async_session is None:
            raise Exception("must be called `init_app` or `init_db`")
        async with self._async_session() as session:
            try:
                yield session
            finally:
                await session.close()

    async def execute_async_query(self, query, params: Optional[dict] = None):
        """비동기 쿼리 실행 헬퍼"""
        async with self._async_session() as session:
            try:
                result = await session.execute(query, params)
                await session.commit()
                return result
            except Exception as e:
                await session.rollback()
                raise e

    @property
    def session(self):
        return self.get_db

    @property
    def async_session(self):
        return self.get_async_db

    @property
    def engine(self):
        return self._engine

    @property
    def async_engine(self):
        return self._async_engine


class SQLAlchemy(BaseSQLAlchemy):
    def __init__(self, app: FastAPI = None, **kwargs):
        url_key = "DB_URL"
        super().__init__(app, url_key, **kwargs)


class SQLAlchemyService(BaseSQLAlchemy):
    def __init__(self, app: FastAPI = None, **kwargs):
        url_key = "DB_SERVICE_URL"
        super().__init__(app, url_key, **kwargs)


class SQLAlchemyUser(BaseSQLAlchemy):
    def __init__(self, app: FastAPI = None, **kwargs):
        url_key = "DB_USER_URL"
        super().__init__(app, url_key, **kwargs)


db = SQLAlchemy()
db_service = SQLAlchemyService()
db_user = SQLAlchemyUser()
