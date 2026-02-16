from __future__ import annotations

import asyncio
import json
from typing import List, Set

from fastapi import WebSocket
from redis import asyncio as aioredis

from app.core.config import get_settings

settings = get_settings()


class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self.active_connections.add(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self.active_connections.discard(websocket)

    async def broadcast(self, message: str) -> None:
        async with self._lock:
            connections: List[WebSocket] = list(self.active_connections)
        for connection in connections:
            try:
                await connection.send_text(message)
            except Exception:
                # 忽略单个连接的异常，并尝试移除
                await self.disconnect(connection)


async def redis_subscriber(manager: ConnectionManager) -> None:
    """
    在应用启动时运行的后台任务：
    - 订阅 Redis `warnings_channel`
    - 收到消息后通过 WebSocket 广播给前端
    """
    redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    pubsub = redis.pubsub()
    await pubsub.subscribe("warnings_channel")

    try:
        async for message in pubsub.listen():
            if message.get("type") != "message":
                continue
            data = message.get("data")
            # 简单校验，可按需扩展
            try:
                json.loads(data)
            except Exception:
                continue
            await manager.broadcast(data)
    finally:
        await pubsub.unsubscribe("warnings_channel")
        await pubsub.close()
        await redis.close()

