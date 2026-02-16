from __future__ import annotations

import asyncio
import logging

from fastapi import Depends, FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db import Base, engine, get_db
from app.routes import regions as regions_router
from app.routes import warnings as warnings_router
from app.websocket_manager import ConnectionManager, redis_subscriber

settings = get_settings()
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

app = FastAPI(title="Geological Hazard Warning System")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(regions_router.router)
app.include_router(warnings_router.router)

manager = ConnectionManager()


@app.on_event("startup")
async def on_startup() -> None:
    # 创建表（演示环境下直接创建，生产环境建议使用 Alembic 迁移）
    Base.metadata.create_all(bind=engine)
    # 启动 Redis 订阅后台任务
    asyncio.create_task(redis_subscriber(manager))


@app.websocket("/ws/warnings")
async def websocket_warnings(websocket: WebSocket, db: Session = Depends(get_db)) -> None:  # noqa: B008
    await manager.connect(websocket)
    try:
        # 简单实现：当前端发送任何消息时忽略，仅用于保持连接
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await manager.disconnect(websocket)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}

