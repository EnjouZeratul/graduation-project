from __future__ import annotations

import asyncio
import logging

from celery import Celery
from celery.schedules import crontab

from app.core.config import get_settings
from app.routes.warnings import run_and_persist_warning_workflow

settings = get_settings()
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

celery_app = Celery(
    "geological_hazard_tasks",
    broker=settings.redis_url,
    backend=settings.redis_url,
)


@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):  # type: ignore[no-untyped-def]
    sender.add_periodic_task(
        crontab(minute="*/30"),
        run_warning_workflow_task.s(),
        name="run warning workflow every 30 minutes (force llm)",
    )


@celery_app.task
def run_warning_workflow_task() -> None:
    asyncio.run(run_and_persist_warning_workflow(force_llm=True))
