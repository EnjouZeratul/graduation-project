from __future__ import annotations

from datetime import datetime
from typing import List

import httpx

from app.core.config import get_settings
from app.schemas import JiusiWorkflowInput, JiusiWorkflowOutput

settings = get_settings()


async def call_jiusi_workflow(
    *, timestamp: datetime, regions: List[str]
) -> JiusiWorkflowOutput:
    """
    调用九思协调 Agent 工作流。

    这里预留真实调用接口，你在九思平台上配置好工作流后：
    - 将 settings.jiusi_api_base 指向该工作流的 HTTP 触发地址
    - 将 settings.jiusi_api_key 配置为工作流的访问密钥（如果需要）
    - 根据实际返回结构，调整 JiusiWorkflowOutput / JiusiWarningResult 的字段映射
    """

    payload = JiusiWorkflowInput(timestamp=timestamp, regions=regions)

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {settings.jiusi_api_key}",
    }

    async with httpx.AsyncClient(base_url=str(settings.jiusi_api_base), timeout=30.0) as client:
        # 假设九思工作流被配置为 POST /workflow/geohazard
        # 你可以在九思平台中将该路径改为实际可用路径
        response = await client.post(
            "/workflow/geohazard",
            json=payload.model_dump(mode="json"),
            headers=headers,
        )
        response.raise_for_status()
        data = response.json()

    # 这里假定九思返回的数据结构与 JiusiWorkflowOutput 一致
    # 如果不一致，你需要在这里做字段映射/转换
    return JiusiWorkflowOutput.model_validate(data)

