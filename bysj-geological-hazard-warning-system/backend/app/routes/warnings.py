from __future__ import annotations

import asyncio
import json
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from redis import Redis
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.agents.graph import RegionInput, run_warning_workflow
from app.agents.llm_provider import build_llm_runtime
from app.agents.data_sources import reset_scraper_runtime_state
from app.core.config import get_settings
from app.db import SessionLocal, get_db_fastapi as get_db
from app.models import Region, Warning
from app.schemas import (
    AbortWorkflowResponse,
    CleanupTestWarningsResponse,
    DebugRandomizeResponse,
    RegionChatRequest,
    RegionChatResponse,
    TriggerWorkflowAsyncResponse,
    TriggerWorkflowResponse,
    WorkflowStatusResponse,
    WarningBase,
    WarningListResponse,
)
from app.warning_filters import is_test_warning

router = APIRouter(prefix="/api/warnings", tags=["warnings"])
settings = get_settings()
llm_runtime = build_llm_runtime(settings)
logger = logging.getLogger(__name__)

last_workflow_state: Dict[str, Any] = {}
workflow_state_lock = asyncio.Lock()
workflow_runtime_state: Dict[str, Any] = {
    "running": False,
    "current_request_id": None,
    "current_started_at": None,
    "last_started_at": None,
    "last_finished_at": None,
    "last_error": None,
    "last_trigger": None,
    "last_processed_regions": 0,
    "last_timestamp": None,
    "total_regions": 0,
    "selected_regions": 0,
    "current_region_limit": None,
}
WORKFLOW_LOCK_KEY = "ghws:workflow:running"
LAST_WORKFLOW_STATE_KEY = "ghws:workflow:last_state"
WORKFLOW_ABORT_KEY_PREFIX = "ghws:workflow:abort:"
WORKFLOW_HEARTBEAT_KEY_PREFIX = "ghws:workflow:hb:"

workflow_abort_flags: set[str] = set()


def _abort_key(request_id: str) -> str:
    return f"{WORKFLOW_ABORT_KEY_PREFIX}{request_id}"


def _hb_key(request_id: str) -> str:
    return f"{WORKFLOW_HEARTBEAT_KEY_PREFIX}{request_id}"


def _touch_heartbeat(request_id: str | None) -> None:
    """
    Heartbeat exists to detect stale Redis locks after uvicorn --reload restarts.
    """
    if not request_id:
        return
    try:
        redis_client = Redis.from_url(settings.redis_url)
        # Short TTL so stale locks self-heal quickly; a background heartbeat loop refreshes it.
        redis_client.set(_hb_key(request_id), datetime.utcnow().isoformat(), ex=3 * 60)
    except Exception:
        pass


def _clear_heartbeat(request_id: str | None) -> None:
    if not request_id:
        return
    try:
        redis_client = Redis.from_url(settings.redis_url)
        redis_client.delete(_hb_key(request_id))
    except Exception:
        pass


def _redis_lock_owner() -> str | None:
    try:
        redis_client = Redis.from_url(settings.redis_url)
        raw_lock = redis_client.get(WORKFLOW_LOCK_KEY)
        if not raw_lock:
            return None
        return raw_lock.decode("utf-8") if isinstance(raw_lock, (bytes, bytearray)) else str(raw_lock)
    except Exception:
        return None


def _redis_has_heartbeat(request_id: str | None) -> bool:
    if not request_id:
        return False
    try:
        redis_client = Redis.from_url(settings.redis_url)
        return bool(redis_client.exists(_hb_key(request_id)))
    except Exception:
        return False


def _maybe_clear_stale_redis_lock() -> bool:
    """
    Clear Redis lock if it exists but has no heartbeat.
    This prevents "ghost running" after server reloads/restarts.
    """
    owner = _redis_lock_owner()
    if not owner:
        return False
    if _redis_has_heartbeat(owner):
        return False
    try:
        redis_client = Redis.from_url(settings.redis_url)
        redis_client.delete(WORKFLOW_LOCK_KEY)
    except Exception:
        return False
    return True

def _set_abort_flag(request_id: str) -> None:
    if not request_id:
        return
    workflow_abort_flags.add(request_id)
    try:
        redis_client = Redis.from_url(settings.redis_url)
        # Keep a short TTL so stale keys don't linger.
        redis_client.set(_abort_key(request_id), "1", ex=6 * 60 * 60)
    except Exception:
        pass


def _clear_abort_flag(request_id: str | None) -> None:
    if not request_id:
        return
    workflow_abort_flags.discard(request_id)
    try:
        redis_client = Redis.from_url(settings.redis_url)
        redis_client.delete(_abort_key(request_id))
    except Exception:
        pass


def _is_abort_requested(request_id: str | None) -> bool:
    if not request_id:
        return False
    if request_id in workflow_abort_flags:
        return True
    try:
        redis_client = Redis.from_url(settings.redis_url)
        return bool(redis_client.exists(_abort_key(request_id)))
    except Exception:
        return False


def _persist_last_state_to_redis(state: Dict[str, Any]) -> None:
    try:
        redis_client = Redis.from_url(settings.redis_url)
        redis_client.set(
            LAST_WORKFLOW_STATE_KEY,
            json.dumps(state, ensure_ascii=False),
            ex=24 * 60 * 60,
        )
    except Exception:
        pass


def _load_last_state_from_redis() -> Dict[str, Any] | None:
    try:
        redis_client = Redis.from_url(settings.redis_url)
        cached = redis_client.get(LAST_WORKFLOW_STATE_KEY)
        if not cached:
            return None
        if isinstance(cached, (bytes, bytearray)):
            text = cached.decode("utf-8", errors="ignore")
        else:
            text = str(cached)
        parsed = json.loads(text or "{}")
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return None
    return None


def _compute_timeout_seconds(*, expected_regions: int | None) -> int:
    base_timeout = max(60, int(settings.workflow_max_runtime_seconds or 420))
    timeout_seconds = base_timeout
    if expected_regions and expected_regions > 0:
        interval = max(0.2, float(settings.scraper_request_interval_seconds or 1.0))
        retries = max(1, int(settings.scraper_max_retries or 1))
        retry_factor = 1.0 + max(0, retries - 1) * 0.35
        # Conservative overhead: DB commits + parsing + occasional scraper cooldown.
        estimated = int(expected_regions * (interval + 0.35) * retry_factor + 300)
        timeout_seconds = max(base_timeout, min(6 * 60 * 60, estimated))
    return int(timeout_seconds)


def _reset_runtime_state(*, error: str | None = None) -> None:
    workflow_runtime_state["running"] = False
    workflow_runtime_state["current_request_id"] = None
    workflow_runtime_state["current_started_at"] = None
    workflow_runtime_state["last_finished_at"] = datetime.utcnow()
    workflow_runtime_state["last_error"] = error
    workflow_runtime_state["current_region_limit"] = None
    workflow_runtime_state["total_regions"] = 0
    workflow_runtime_state["selected_regions"] = 0


def _extract_confidence(meteorology: str | None) -> float | None:
    if not meteorology:
        return None
    try:
        parsed = json.loads(meteorology)
    except Exception:
        return None
    value = parsed.get("confidence")
    try:
        if value is None:
            return None
        return max(0.0, min(1.0, float(value)))
    except Exception:
        return None


def _random_between(rng: Any, lo: float, hi: float) -> float:
    try:
        return float(lo) + (float(hi) - float(lo)) * float(rng.random())
    except Exception:
        return float(lo)


def _pick_level_from_score(score: float) -> str:
    if score < 25:
        return "green"
    if score < 50:
        return "yellow"
    if score < 75:
        return "orange"
    return "red"


def _infer_hazards_from_features(*, rain_24h: float, rain_1h: float, soil_m: float, slope: float, fault: float) -> list[str]:
    def _clamp(v: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, v))

    landslide_score = 0.45 * min(1.0, slope / 45.0) + 0.35 * min(1.0, rain_24h / 120.0) + 0.2 * _clamp(soil_m, 0, 1)
    debris_flow_score = 0.5 * min(1.0, rain_1h / 40.0) + 0.3 * min(1.0, rain_24h / 120.0) + 0.2 * min(1.0, slope / 45.0)
    collapse_score = 0.6 * min(1.0, slope / 45.0) + 0.4 * min(1.0, 2.0 / max(0.4, fault))
    subsidence_score = 0.7 * min(1.0, 2.0 / max(0.4, fault)) + 0.3 * _clamp(soil_m, 0, 1)

    scored = [
        ("滑坡", landslide_score),
        ("泥石流", debris_flow_score),
        ("崩塌", collapse_score),
        ("地面塌陷", subsidence_score),
    ]
    scored.sort(key=lambda x: x[1], reverse=True)
    return [name for name, _ in scored[:3]]


def _risk_score_from_features(*, rain_24h: float, rain_1h: float, soil_m: float, wind: float, slope: float, fault: float, history_count: float) -> float:
    def _clamp(v: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, v))

    rain_component = min(1.0, rain_24h / 120.0) * 35
    short_rain_component = min(1.0, rain_1h / 40.0) * 20
    soil_component = _clamp(soil_m, 0, 1) * 10
    wind_component = min(1.0, wind / 25.0) * 5
    slope_component = min(1.0, slope / 45.0) * 15
    fault_component = min(1.0, 2.0 / max(0.4, fault)) * 10
    history_component = min(1.0, history_count / 8.0) * 5
    total = rain_component + short_rain_component + soil_component + wind_component + slope_component + fault_component + history_component
    return round(_clamp(total, 0, 100), 2)


def _confidence_from_score(score: float) -> float:
    score = max(0.0, min(100.0, float(score)))
    thresholds = [25.0, 50.0, 75.0]
    nearest = min(abs(score - t) for t in thresholds)
    # nearer threshold => lower confidence
    ambiguity = max(0.0, min(1.0, nearest / 25.0))
    conf = 0.55 + 0.35 * ambiguity
    return max(0.3, min(0.96, round(conf, 3)))


def _build_fallback_answer(
    *,
    region_name: str,
    risk_level: str | None,
    reason: str | None,
    confidence: float | None,
    question: str,
) -> str:
    confidence_text = f"{confidence:.0%}" if confidence is not None else "未知"
    return (
        f"{region_name} 当前风险等级为 {risk_level or '未知'}，置信度约为 {confidence_text}。"
        f"{('最新预警原因：' + reason) if reason else '暂无最新预警原因。'}"
        f"（AI 暂不可用，返回本地说明）你的问题是：{question}"
    )


def _pick_regions_for_run(
    regions: List[Region],
    region_limit: int | None,
    *,
    selection_seed: str | None = None,
) -> tuple[List[Region], int, int]:
    total = len(regions)
    if region_limit is None or int(region_limit) <= 0 or total <= int(region_limit):
        return list(regions), total, total

    limit = max(1, int(region_limit))
    risk_rank = {"red": 0, "orange": 1, "yellow": 2, "green": 3}

    def _priority(item: Region) -> tuple[int, int, int, str]:
        code = str(item.code or "")
        level_rank = int(risk_rank.get(str(item.risk_level or "green"), 4))
        is_city = 0 if (len(code) == 6 and code.endswith("00") and not code.endswith("0000")) else 1
        is_province = 0 if (len(code) == 6 and code.endswith("0000")) else 1
        return (level_rank, is_city, is_province, code)

    ordered = sorted(regions, key=_priority)

    # For fast-mode batches, avoid always taking the first N regions.
    # Strategy:
    # - Always include a small "top risk" head (stable)
    # - Rotate the remainder deterministically based on request_id/seed (covers more regions across repeated runs)
    if not selection_seed:
        selected = ordered[:limit]
        return selected, total, len(selected)

    must = min(limit, max(20, limit // 4))
    head = ordered[:must]
    tail = ordered[must:]
    if not tail or limit <= must:
        selected = ordered[:limit]
        return selected, total, len(selected)

    try:
        seed_int = int(hashlib.sha256(selection_seed.encode("utf-8", errors="ignore")).hexdigest()[:8], 16)
    except Exception:
        seed_int = 0
    offset = seed_int % len(tail)
    rotated = tail[offset:] + tail[:offset]
    selected = head + rotated[: max(0, limit - must)]
    return selected, total, len(selected)


async def run_and_persist_warning_workflow(
    force_llm: bool = True,
    *,
    region_limit: int | None = None,
    request_id: str | None = None,
    max_runtime_seconds: int | None = None,
) -> Dict[str, Any]:
    db = SessionLocal()
    try:
        regions = db.execute(select(Region)).scalars().all()
        # Use request_id to rotate fast-mode selection batches.
        selected_regions, total_regions, selected_count = _pick_regions_for_run(
            regions,
            region_limit,
            selection_seed=request_id,
        )
        region_inputs: List[RegionInput] = [{"name": r.name, "code": r.code} for r in selected_regions]
    finally:
        db.close()

    if not region_inputs:
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "results": [],
            "total_regions": total_regions if "total_regions" in locals() else 0,
            "selected_regions": selected_count if "selected_count" in locals() else 0,
        }

    def _group_by_prefix(items: List[RegionInput]) -> List[List[RegionInput]]:
        groups: Dict[str, List[RegionInput]] = {}
        for it in items:
            code = str(it.get("code") or "")
            prefix = code[:2] if len(code) >= 2 else "__"
            groups.setdefault(prefix, []).append(it)
        # Split large groups into smaller batches so:
        # - partial results flush earlier
        # - user abort can stop sooner (between batches)
        batch_size = max(15, min(40, int(settings.collector_max_concurrency or 12) * 2))
        out: List[List[RegionInput]] = []
        for k in sorted(groups.keys()):
            chunk = groups[k]
            for i in range(0, len(chunk), batch_size):
                out.append(chunk[i : i + batch_size])
        return out

    started_at = datetime.utcnow()
    now = datetime.utcnow()
    groups = _group_by_prefix(region_inputs)

    db_session = SessionLocal()
    try:
        redis_client = Redis.from_url(settings.redis_url)
        _touch_heartbeat(request_id)

        all_results_by_code: Dict[str, Dict[str, Any]] = {}
        processed = 0
        partial = False
        aborted = False

        global last_workflow_state
        last_workflow_state = {
            "request_id": request_id,
            "timestamp": now.isoformat(),
            "results": [],
            "force_llm": force_llm,
            "total_regions": int(total_regions),
            "selected_regions": int(selected_count),
            "processed_regions": 0,
            "partial": True,
        }
        _persist_last_state_to_redis(last_workflow_state)
        if request_id:
            async with workflow_state_lock:
                if workflow_runtime_state.get("current_request_id") == request_id:
                    workflow_runtime_state["last_processed_regions"] = 0

        for group in groups:
            if _is_abort_requested(request_id):
                aborted = True
                partial = True
                break
            if max_runtime_seconds is not None:
                elapsed = (datetime.utcnow() - started_at).total_seconds()
                if elapsed >= max(10, int(max_runtime_seconds)):
                    partial = True
                    break

            _touch_heartbeat(request_id)
            result = await run_warning_workflow(timestamp=now, regions=group, force_llm=force_llm)

            ws_results_chunk: List[Dict[str, Any]] = []
            for item in result.results:
                processed += 1
                region = (
                    db_session.execute(select(Region).where(Region.code == item.region_code))
                    .scalars()
                    .first()
                )
                if region is None:
                    region = Region(name=item.region_name, code=item.region_code, risk_level=item.level)
                    db_session.add(region)
                    db_session.flush()
                else:
                    region.risk_level = item.level
                    region.last_updated_at = now

                met_payload = dict(item.meteorology or {})
                met_payload["confidence"] = float(item.confidence)

                warning = Warning(
                    region_id=region.id,
                    level=item.level,
                    reason=item.reason,
                    meteorology=json.dumps(met_payload, ensure_ascii=False),
                    created_at=now,
                    source="langgraph-hybrid",
                )
                db_session.add(warning)

                row = {
                    "region_id": region.id,
                    "region_code": item.region_code,
                    "region_name": item.region_name,
                    "level": item.level,
                    "reason": item.reason,
                    "confidence": float(item.confidence),
                    "meteorology": met_payload,
                }
                all_results_by_code[str(item.region_code)] = row
                ws_results_chunk.append(row)

            db_session.commit()
            _touch_heartbeat(request_id)

            # Publish incremental updates (delta). Frontend merges by region_code.
            delta_message = {
                "timestamp": now.isoformat(),
                "results": ws_results_chunk,
                "total_regions": int(total_regions),
                "selected_regions": int(selected_count),
                "processed_regions": int(processed),
                "partial": True,
            }
            try:
                redis_client.publish("warnings_channel", json.dumps(delta_message, ensure_ascii=False))
            except Exception:
                pass

            last_workflow_state = {
                "request_id": request_id,
                "timestamp": now.isoformat(),
                "results": list(all_results_by_code.values()),
                "force_llm": force_llm,
                "total_regions": int(total_regions),
                "selected_regions": int(selected_count),
                "processed_regions": int(processed),
                "partial": True,
                "aborted": bool(aborted),
            }
            _persist_last_state_to_redis(last_workflow_state)
            if request_id:
                async with workflow_state_lock:
                    if workflow_runtime_state.get("current_request_id") == request_id:
                        workflow_runtime_state["last_processed_regions"] = int(processed)
                        workflow_runtime_state["last_timestamp"] = datetime.utcnow()
                        workflow_runtime_state["total_regions"] = int(total_regions)
                        workflow_runtime_state["selected_regions"] = int(selected_count)

        # Final snapshot (may be partial if max_runtime_seconds hit)
        final_message = {
            "timestamp": now.isoformat(),
            "results": list(all_results_by_code.values()),
            "total_regions": int(total_regions),
            "selected_regions": int(selected_count),
            "processed_regions": int(processed),
            "partial": bool(partial) or (processed < int(selected_count)),
            "aborted": bool(aborted),
        }
        last_workflow_state = {
            "request_id": request_id,
            **final_message,
            "force_llm": force_llm,
        }
        _persist_last_state_to_redis(last_workflow_state)
        return final_message
    finally:
        _clear_abort_flag(request_id)
        _clear_heartbeat(request_id)
        db_session.close()


async def _run_workflow_in_background(
    *,
    request_id: str,
    force_llm: bool,
    region_limit: int | None = None,
    expected_regions: int | None = None,
) -> None:
    last_error: str | None = None
    last_processed_regions = 0
    last_timestamp: datetime | None = None
    total_regions = 0
    selected_regions = 0

    hb_stop = asyncio.Event()

    async def _hb_loop() -> None:
        # Refresh heartbeat frequently so the lock can be treated as stale quickly after crashes.
        while not hb_stop.is_set():
            _touch_heartbeat(request_id)
            try:
                await asyncio.wait_for(hb_stop.wait(), timeout=20.0)
            except asyncio.TimeoutError:
                continue

    hb_task = asyncio.create_task(_hb_loop())

    try:
        timeout_seconds = _compute_timeout_seconds(expected_regions=expected_regions)
        # Prefer cooperative timeout so partial results are committed/published.
        message = await asyncio.wait_for(
            run_and_persist_warning_workflow(
                force_llm=force_llm,
                region_limit=region_limit,
                request_id=request_id,
                max_runtime_seconds=timeout_seconds,
            ),
            timeout=timeout_seconds + 60,
        )
        last_processed_regions = int(message.get("processed_regions", 0) or len(message.get("results", [])))
        total_regions = int(message.get("total_regions", 0) or 0)
        selected_regions = int(message.get("selected_regions", 0) or 0)
        ts_text = message.get("timestamp")
        if isinstance(ts_text, str):
            try:
                last_timestamp = datetime.fromisoformat(ts_text)
            except ValueError:
                last_timestamp = None
        if bool(message.get("aborted")):
            last_error = "manual_abort"
        elif bool(message.get("partial")):
            last_error = f"workflow_partial_timeout_after_{timeout_seconds}s"
    except asyncio.TimeoutError:
        # Hard timeout: still try to report partial progress if any was persisted.
        last_error = f"workflow_timeout_after_{timeout_seconds}s"
        if last_workflow_state.get("request_id") == request_id:
            try:
                last_processed_regions = int(last_workflow_state.get("processed_regions", 0) or len(last_workflow_state.get("results", [])))
            except Exception:
                pass
    except Exception as exc:
        last_error = str(exc)[:500]
    finally:
        hb_stop.set()
        try:
            hb_task.cancel()
        except Exception:
            pass
        _clear_abort_flag(request_id)
        _clear_heartbeat(request_id)
        async with workflow_state_lock:
            if workflow_runtime_state.get("current_request_id") == request_id:
                workflow_runtime_state["running"] = False
                workflow_runtime_state["current_request_id"] = None
                workflow_runtime_state["current_started_at"] = None
            workflow_runtime_state["last_finished_at"] = datetime.utcnow()
            workflow_runtime_state["last_error"] = last_error
            workflow_runtime_state["last_processed_regions"] = int(last_processed_regions)
            workflow_runtime_state["last_timestamp"] = last_timestamp
            if total_regions > 0:
                workflow_runtime_state["total_regions"] = total_regions
            if selected_regions > 0:
                workflow_runtime_state["selected_regions"] = selected_regions

        # Release distributed workflow lock if owned by current request.
        try:
            redis_client = Redis.from_url(settings.redis_url)
            raw_lock = redis_client.get(WORKFLOW_LOCK_KEY)
            lock_owner = raw_lock.decode("utf-8") if isinstance(raw_lock, (bytes, bytearray)) else str(raw_lock or "")
            if lock_owner == request_id:
                redis_client.delete(WORKFLOW_LOCK_KEY)
        except Exception:
            pass


async def _schedule_background_workflow(
    *, force_llm: bool, trigger: str, region_limit: int | None = None
) -> tuple[bool, str | None, datetime | None]:
    request_id = str(uuid4())
    started_at = datetime.utcnow()
    max_runtime = max(60, int(settings.workflow_max_runtime_seconds or 420))
    total_regions = 0
    selected_regions = 0

    stale_reset = False
    async with workflow_state_lock:
        if workflow_runtime_state["running"]:
            started_at_current = workflow_runtime_state.get("current_started_at")
            stale_threshold = timedelta(seconds=max_runtime + 60)
            if (
                isinstance(started_at_current, datetime)
                and datetime.utcnow() - started_at_current > stale_threshold
            ):
                _reset_runtime_state(error="stale_workflow_state_reset")
                stale_reset = True
            else:
                return False, None, None

    if stale_reset:
        try:
            redis_client = Redis.from_url(settings.redis_url)
            redis_client.delete(WORKFLOW_LOCK_KEY)
            logger.warning("stale workflow runtime state reset, lock cleared")
        except Exception:
            pass

    # Determine expected workload to scale lock TTL and timeout estimates.
    try:
        db = SessionLocal()
        total_regions = int(db.execute(select(func.count(Region.id))).scalar_one() or 0)
    except Exception:
        total_regions = 0
    finally:
        try:
            db.close()
        except Exception:
            pass

    if region_limit is None or int(region_limit) <= 0:
        selected_regions = int(total_regions)
    else:
        selected_regions = int(min(total_regions, int(region_limit)))

    # Scale lock TTL to expected runtime so it doesn't expire mid-run.
    try:
        max_runtime = _compute_timeout_seconds(expected_regions=selected_regions)
    except Exception:
        pass

    # Acquire distributed lock to avoid duplicate background runs across workers/processes.
    try:
        redis_client = Redis.from_url(settings.redis_url)
        if not bool(redis_client.set(WORKFLOW_LOCK_KEY, request_id, nx=True, ex=max_runtime + 120)):
            # With uvicorn --reload, the server can restart mid-run and leave a lock behind.
            # If the lock owner has no heartbeat, we consider it stale and clear it once.
            if _maybe_clear_stale_redis_lock():
                if not bool(redis_client.set(WORKFLOW_LOCK_KEY, request_id, nx=True, ex=max_runtime + 120)):
                    return False, None, None
            else:
                return False, None, None
    except Exception:
        # Fallback to in-memory guard when Redis is temporarily unavailable.
        pass

    async with workflow_state_lock:
        workflow_runtime_state["running"] = True
        workflow_runtime_state["current_request_id"] = request_id
        workflow_runtime_state["current_started_at"] = started_at
        workflow_runtime_state["last_started_at"] = started_at
        workflow_runtime_state["last_finished_at"] = None
        workflow_runtime_state["last_error"] = None
        workflow_runtime_state["last_trigger"] = trigger
        workflow_runtime_state["current_region_limit"] = region_limit
        workflow_runtime_state["total_regions"] = total_regions
        workflow_runtime_state["selected_regions"] = selected_regions

    _touch_heartbeat(request_id)
    asyncio.create_task(
        _run_workflow_in_background(
            request_id=request_id,
            force_llm=force_llm,
            region_limit=region_limit,
            expected_regions=selected_regions,
        )
    )
    return True, request_id, started_at


def _build_workflow_status_response() -> WorkflowStatusResponse:
    # If this process thinks it's idle but Redis lock exists, it may be a lock owned by another process,
    # or a stale lock after restart. Heartbeat lets us distinguish them.
    if not bool(workflow_runtime_state.get("running")):
        owner = _redis_lock_owner()
        if owner:
            if not _redis_has_heartbeat(owner):
                _maybe_clear_stale_redis_lock()
            else:
                workflow_runtime_state["running"] = True
                workflow_runtime_state["current_request_id"] = owner
                # started_at can be unknown after restart; keep None to avoid misleading elapsed.
                workflow_runtime_state["current_started_at"] = workflow_runtime_state.get("current_started_at") or None

    current_started_at = workflow_runtime_state.get("current_started_at")
    elapsed = 0
    if isinstance(current_started_at, datetime):
        elapsed = max(0, int((datetime.utcnow() - current_started_at).total_seconds()))

    # While running, refresh progress from persisted last state so UI can show
    # "已处理 X 个地区" in near real-time across polling cycles.
    if bool(workflow_runtime_state.get("running")):
        cached = _load_last_state_from_redis()
        if isinstance(cached, dict):
            rid = str(workflow_runtime_state.get("current_request_id") or "")
            if rid and str(cached.get("request_id") or "") == rid:
                try:
                    processed = int(cached.get("processed_regions", 0) or 0)
                    if processed >= int(workflow_runtime_state.get("last_processed_regions", 0) or 0):
                        workflow_runtime_state["last_processed_regions"] = processed
                except Exception:
                    pass
                try:
                    selected = int(cached.get("selected_regions", 0) or 0)
                    total = int(cached.get("total_regions", 0) or 0)
                    if selected > 0:
                        workflow_runtime_state["selected_regions"] = selected
                    if total > 0:
                        workflow_runtime_state["total_regions"] = total
                except Exception:
                    pass

    return WorkflowStatusResponse(
        running=bool(workflow_runtime_state.get("running")),
        current_request_id=workflow_runtime_state.get("current_request_id"),
        current_started_at=current_started_at,
        last_started_at=workflow_runtime_state.get("last_started_at"),
        last_finished_at=workflow_runtime_state.get("last_finished_at"),
        last_error=workflow_runtime_state.get("last_error"),
        last_trigger=workflow_runtime_state.get("last_trigger"),
        last_processed_regions=int(workflow_runtime_state.get("last_processed_regions", 0) or 0),
        last_timestamp=workflow_runtime_state.get("last_timestamp"),
        total_regions=int(workflow_runtime_state.get("total_regions", 0) or 0),
        selected_regions=int(workflow_runtime_state.get("selected_regions", 0) or 0),
        current_elapsed_seconds=elapsed,
    )


@router.get("", response_model=WarningListResponse)
def list_warnings(db: Session = Depends(get_db)) -> WarningListResponse:
    stmt = select(Warning).order_by(Warning.created_at.desc()).limit(600)
    warnings = db.execute(stmt).scalars().all()

    items = []
    for warning in warnings:
        if is_test_warning(source=warning.source, reason=warning.reason):
            continue
        warning_dict = WarningBase.model_validate(warning).model_dump()
        warning_dict["confidence"] = _extract_confidence(warning.meteorology)
        region = db.query(Region).filter(Region.id == warning.region_id).first()
        if region:
            warning_dict["region_name"] = region.name
        items.append(warning_dict)
        if len(items) >= 200:
            break

    return WarningListResponse(items=items, total=len(items))


@router.post("/trigger", response_model=TriggerWorkflowResponse)
async def trigger_warning_workflow() -> TriggerWorkflowResponse:
    message = await run_and_persist_warning_workflow(force_llm=True)
    results = []
    for item in message.get("results", []):
        results.append(
            {
                "region_name": item.get("region_name"),
                "region_code": item.get("region_code"),
                "level": item.get("level"),
                "reason": item.get("reason"),
                "confidence": item.get("confidence", 0.5),
                "meteorology": item.get("meteorology", {}),
            }
        )
    return TriggerWorkflowResponse(
        timestamp=datetime.fromisoformat(message["timestamp"]),
        processed_regions=len(results),
        results=results,
    )


@router.post("/trigger/async", response_model=TriggerWorkflowAsyncResponse)
async def trigger_warning_workflow_async(
    fast_mode: bool = False,
    region_limit: int | None = None,
) -> TriggerWorkflowAsyncResponse:
    computed_region_limit: int | None = None
    if fast_mode:
        default_limit = max(1, int(settings.workflow_manual_region_limit or 0))
        if region_limit is None:
            computed_region_limit = default_limit
        else:
            computed_region_limit = max(1, min(500, int(region_limit)))

    accepted, request_id, started_at = await _schedule_background_workflow(
        force_llm=True,
        trigger="manual_fast" if fast_mode else "manual_full",
        region_limit=computed_region_limit,
    )

    if not accepted:
        owner = _redis_lock_owner()
        return TriggerWorkflowAsyncResponse(
            accepted=False,
            running=True,
            message="已有主动刷新任务在运行，请稍后查看状态。",
            started_at=workflow_runtime_state.get("current_started_at"),
            request_id=workflow_runtime_state.get("current_request_id") or owner,
        )

    return TriggerWorkflowAsyncResponse(
        accepted=True,
        running=True,
        message=(
            f"已启动主动刷新（快速模式，最多 {computed_region_limit} 个地区），完成后会自动推送最新预警。"
            if computed_region_limit
            else "已启动主动刷新（全量模式），完成后会自动推送最新预警。"
        ),
        started_at=started_at,
        request_id=request_id,
    )


@router.get("/trigger/status", response_model=WorkflowStatusResponse)
async def get_trigger_workflow_status() -> WorkflowStatusResponse:
    async with workflow_state_lock:
        return _build_workflow_status_response()


@router.post("/trigger/reset", response_model=WorkflowStatusResponse)
async def reset_trigger_workflow_state() -> WorkflowStatusResponse:
    current = workflow_runtime_state.get("current_request_id")
    _clear_abort_flag(str(current) if current else None)
    _clear_heartbeat(str(current) if current else None)
    async with workflow_state_lock:
        _reset_runtime_state(error="manual_reset")
        workflow_runtime_state["last_trigger"] = "manual_reset"

    try:
        redis_client = Redis.from_url(settings.redis_url)
        redis_client.delete(WORKFLOW_LOCK_KEY)
    except Exception:
        pass

    async with workflow_state_lock:
        return _build_workflow_status_response()


@router.post("/trigger/abort", response_model=AbortWorkflowResponse)
async def abort_trigger_workflow() -> AbortWorkflowResponse:
    async with workflow_state_lock:
        if not bool(workflow_runtime_state.get("running")):
            return AbortWorkflowResponse(ok=True, running=False, message="当前没有正在运行的主动刷新任务。", request_id=None)
        request_id = str(workflow_runtime_state.get("current_request_id") or "")
        if request_id:
            _set_abort_flag(request_id)
        return AbortWorkflowResponse(
            ok=True,
            running=True,
            request_id=request_id or None,
            message="已请求中止主动刷新：当前批次结束后将停止，并保留已处理结果。",
        )


@router.post("/debug/reset-scraper-runtime", response_model=dict)
async def reset_scraper_runtime(clear_cache: bool = False) -> dict:
    payload = reset_scraper_runtime_state(clear_cache=clear_cache)
    return {
        "ok": True,
        "clear_cache": bool(clear_cache),
        "runtime": payload,
    }


@router.post("/debug/randomize", response_model=DebugRandomizeResponse)
async def debug_randomize_all_regions() -> DebugRandomizeResponse:
    """
    Debug-only: generate a full random snapshot for all regions and push it via WS.
    - Does NOT call LangGraph/LLM/official APIs/scrapers.
    - Does NOT persist into DB (won't affect history counters).
    """
    async with workflow_state_lock:
        if bool(workflow_runtime_state.get("running")):
            return DebugRandomizeResponse(
                ok=False,
                message="当前有主动刷新任务在运行，请先中止或等待结束后再使用随机模拟。",
                timestamp=datetime.utcnow().isoformat(),
                total_regions=0,
                results=[],
            )

    now = datetime.utcnow()
    db = SessionLocal()
    try:
        regions = db.execute(select(Region)).scalars().all()
        results: List[Dict[str, Any]] = []
        for r in regions:
            code = str(r.code or "")
            # True random each click (seeded by time + region code)
            rng = hashlib.sha256(f"{now.isoformat()}::{code}".encode("utf-8", errors="ignore")).digest()
            # use small deterministic PRNG from digest but unique per click
            seed = int.from_bytes(rng[:8], "big", signed=False)
            import random as _random

            rr = _random.Random(seed)

            rain_24h = round(_random_between(rr, 0, 180) ** 0.85, 1)
            rain_1h = round(min(60.0, _random_between(rr, 0, 50) ** 0.9), 1)
            humidity = int(round(_random_between(rr, 35, 98), 0))
            wind_speed = round(_random_between(rr, 0.2, 16.0), 1)
            soil_moisture = round(_random_between(rr, 0.05, 0.85), 2)
            slope = round(_random_between(rr, 1.0, 42.0), 1)
            fault_distance = round(_random_between(rr, 0.6, 50.0), 1)
            history_count = int(round(_random_between(rr, 0, 6), 0))

            risk_score = _risk_score_from_features(
                rain_24h=rain_24h,
                rain_1h=rain_1h,
                soil_m=soil_moisture,
                wind=wind_speed,
                slope=slope,
                fault=fault_distance,
                history_count=float(history_count),
            )
            level = _pick_level_from_score(risk_score)
            confidence = _confidence_from_score(risk_score)
            hazards = _infer_hazards_from_features(
                rain_24h=rain_24h,
                rain_1h=rain_1h,
                soil_m=soil_moisture,
                slope=slope,
                fault=fault_distance,
            )

            meteorology = {
                "rain_24h": rain_24h,
                "rain_1h": rain_1h,
                "humidity": humidity,
                "wind_speed": wind_speed,
                "soil_moisture": soil_moisture,
                "slope": slope,
                "fault_distance": fault_distance,
                "hazard_candidates": hazards,
                "confidence": confidence,
                "risk_score": risk_score,
                "data_mode": "random_simulated",
                "source_status": {
                    "success": {"meteorology": ["debug_random"], "geology": ["debug_random"]},
                    "errors": {},
                },
                "confidence_reason": "随机模拟数据：用于前端演示与快速联调，不写入历史库。",
                "confidence_breakdown": {
                    "formula": "随机模拟：按风险分距离阈值的远近生成置信度（越远越高）",
                    "final_confidence": confidence,
                },
            }

            reason = (
                f"随机模拟：24h降雨{rain_24h:.1f}mm, 1h降雨{rain_1h:.1f}mm, 湿度{humidity}%, "
                f"风速{wind_speed:.1f}m/s, 坡度{slope:.1f}°, 断层距离{fault_distance:.1f}km, "
                f"历史主动预警次数{history_count}次；最可能灾害：{'、'.join(hazards)}"
            )

            results.append(
                {
                    "region_id": int(r.id),
                    "region_code": code,
                    "region_name": str(r.name or code),
                    "level": level,
                    "reason": reason,
                    "confidence": float(confidence),
                    "meteorology": meteorology,
                }
            )
    finally:
        db.close()

    payload = {
        "timestamp": now.isoformat(),
        "results": results,
        "total_regions": len(results),
        "selected_regions": len(results),
        "processed_regions": len(results),
        "partial": False,
    }
    try:
        redis_client = Redis.from_url(settings.redis_url)
        redis_client.publish("warnings_channel", json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass

    # Also update in-memory/redis "last_workflow_state" cache for debugging convenience,
    # but we do NOT mark workflow running nor persist warnings into DB.
    global last_workflow_state
    last_workflow_state = payload
    _persist_last_state_to_redis(payload)

    return DebugRandomizeResponse(
        ok=True,
        message="已生成全量随机模拟数据并推送到前端（不入库）。",
        timestamp=payload["timestamp"],
        total_regions=len(results),
        results=results,
    )


@router.post("/cleanup-test-data", response_model=CleanupTestWarningsResponse)
def cleanup_test_warnings(db: Session = Depends(get_db)) -> CleanupTestWarningsResponse:
    all_warnings = db.execute(select(Warning)).scalars().all()

    test_warning_ids: List[int] = []
    affected_region_ids: set[int] = set()
    for warning in all_warnings:
        if is_test_warning(source=warning.source, reason=warning.reason):
            test_warning_ids.append(int(warning.id))
            affected_region_ids.add(int(warning.region_id))

    if not test_warning_ids:
        return CleanupTestWarningsResponse(
            deleted_warnings=0,
            affected_regions=0,
            message="未发现测试预警数据。",
        )

    db.query(Warning).filter(Warning.id.in_(test_warning_ids)).delete(synchronize_session=False)

    now = datetime.utcnow()
    for region_id in affected_region_ids:
        region = db.get(Region, region_id)
        if region is None:
            continue

        candidates = (
            db.execute(
                select(Warning)
                .where(Warning.region_id == region_id)
                .order_by(Warning.created_at.desc())
                .limit(20)
            )
            .scalars()
            .all()
        )
        latest_non_test = next(
            (
                w
                for w in candidates
                if not is_test_warning(source=w.source, reason=w.reason)
            ),
            None,
        )

        if latest_non_test:
            region.risk_level = latest_non_test.level
            region.last_updated_at = latest_non_test.created_at
        else:
            region.risk_level = "green"
            region.last_updated_at = now

    db.commit()
    return CleanupTestWarningsResponse(
        deleted_warnings=len(test_warning_ids),
        affected_regions=len(affected_region_ids),
        message="测试预警数据清理完成。",
    )

@router.post("/chat", response_model=RegionChatResponse)
async def chat_with_region_warning_context(
    payload: RegionChatRequest,
    db: Session = Depends(get_db),
) -> RegionChatResponse:
    region = (
        db.execute(select(Region).where(Region.code == payload.region_code).limit(1))
        .scalars()
        .first()
    )
    if region is None:
        raise HTTPException(status_code=404, detail="Region not found")

    warning_candidates = (
        db.execute(
            select(Warning)
            .where(Warning.region_id == region.id)
            .order_by(Warning.created_at.desc())
            .limit(20)
        )
        .scalars()
        .all()
    )
    latest_warning = next(
        (
            w
            for w in warning_candidates
            if not is_test_warning(source=w.source, reason=w.reason)
        ),
        None,
    )

    risk_level = latest_warning.level if latest_warning else region.risk_level
    reason = latest_warning.reason if latest_warning else None
    meteorology: Dict[str, Any] = {}
    if latest_warning and latest_warning.meteorology:
        try:
            meteorology = json.loads(latest_warning.meteorology)
        except json.JSONDecodeError:
            meteorology = {}
    confidence = _extract_confidence(latest_warning.meteorology if latest_warning else None)

    history_messages: List[Dict[str, str]] = [
        {"role": item.role, "content": item.content[:800]} for item in payload.history[-6:]
    ]

    compact_context = {
        "region_name": region.name,
        "region_code": region.code,
        "risk_level": risk_level,
        "confidence": confidence,
        "latest_warning_reason": reason,
        "latest_meteorology": {
            "rain_24h": meteorology.get("rain_24h"),
            "rain_1h": meteorology.get("rain_1h"),
            "soil_moisture": meteorology.get("soil_moisture"),
            "wind_speed": meteorology.get("wind_speed"),
        },
    }

    if not llm_runtime:
        return RegionChatResponse(
            region_code=region.code,
            answer=_build_fallback_answer(
                region_name=region.name,
                risk_level=risk_level,
                reason=reason,
                confidence=confidence,
                question=payload.question,
            ),
            risk_level=risk_level,
            generated_at=datetime.utcnow(),
        )

    system_prompt = (
        "你是地质灾害预警问答助手。只能依据提供的上下文回答。"
        "如果信息不足，要明确说明不确定性和缺失数据。"
        "输出简洁、可执行的中文建议。"
    )
    user_content = json.dumps({"question": payload.question, "context": compact_context}, ensure_ascii=False)

    try:
        response = await llm_runtime.client.chat.completions.create(
            model=llm_runtime.model,
            messages=[
                {"role": "system", "content": system_prompt},
                *history_messages,
                {"role": "user", "content": user_content},
            ],
            temperature=0.2,
            max_tokens=260,
        )
        answer = (response.choices[0].message.content or "").strip()
        if not answer:
            answer = _build_fallback_answer(
                region_name=region.name,
                risk_level=risk_level,
                reason=reason,
                confidence=confidence,
                question=payload.question,
            )
    except Exception:
        answer = _build_fallback_answer(
            region_name=region.name,
            risk_level=risk_level,
            reason=reason,
            confidence=confidence,
            question=payload.question,
        )

    return RegionChatResponse(
        region_code=region.code,
        answer=answer,
        risk_level=risk_level,
        generated_at=datetime.utcnow(),
    )


@router.get("/debug/last-collection", response_model=dict)
async def get_last_collection() -> dict:
    if last_workflow_state.get("results"):
        return last_workflow_state
    cached = _load_last_state_from_redis()
    if cached:
        return cached
    return last_workflow_state


