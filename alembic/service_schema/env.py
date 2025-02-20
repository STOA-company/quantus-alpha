from logging.config import fileConfig
from sqlalchemy import engine_from_config
from sqlalchemy import pool
from alembic import context, op
import os
import sys
import re

# 프로젝트 루트 경로 추가 (중요!)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models import ServiceBase
from app.core.config import settings

# Alembic Config object
config = context.config

# Logging configuration
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Target metadata for migrations
target_metadata = ServiceBase.metadata

# Set database URL
config.set_main_option("sqlalchemy.url", settings.SERVICE_DATABASE_URL)


def generate_slug(rev):
    # 마이그레이션 디렉토리 경로
    migrations_dir = os.path.dirname(__file__)

    # 기존 마이그레이션 파일들 찾기
    existing_migrations = [f for f in os.listdir(migrations_dir) if f.startswith(rev)]

    # 현재 최대 버전 번호 찾기
    max_version = 0
    for migration in existing_migrations:
        match = re.search(f"{rev}_v(\d+)", migration)
        if match:
            max_version = max(max_version, int(match.group(1)))

    # 다음 버전 번호
    next_version = max_version + 1

    return f"{rev}_v{next_version}"


# Alembic configuration에 이 함수 적용
op.generate_slug = generate_slug


def include_object(object, name, type_, reflected, compare_to):
    # 테이블 타입인 경우에만 체크
    if type_ == "table":
        # 모델에 정의된 테이블만 마이그레이션에 포함
        return name in ServiceBase.metadata.tables.keys()
    return True


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    url = url.replace("mysql://", "mysql+pymysql://")
    config.set_main_option("sqlalchemy.url", url)
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,  # 모델에 정의된 테이블만 포함
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,  # 모델에 정의된 테이블만 포함
            compare_type=True,
            compare_server_default=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
