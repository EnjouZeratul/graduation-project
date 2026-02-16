
from contextlib import contextmanager
from typing import Generator, AsyncGenerator
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from app.core.config import get_settings

settings = get_settings()

# Sync engine for traditional operations / 同步引擎用于传统操作
engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Async engine placeholder for future scaling / 异步引擎占位符用于未来扩展
# async_engine = create_async_engine(settings.async_database_url, pool_pre_ping=True)
# AsyncSessionLocal = async_sessionmaker(async_engine, autocommit=False, autoflush=False)

Base = declarative_base()

@contextmanager
def get_db() -> Generator[Session, None, None]:
    """Context manager pattern for complex transactions / 复杂事务的上下文管理器模式"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_db_fastapi() -> Generator[Session, None, None]:
    """FastAPI dependency injection pattern / FastAPI 依赖注入模式"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Future async support / 未来异步支持
# async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
#     async with AsyncSessionLocal() as session:
#         yield session
