from __future__ import annotations

import asyncio
import hashlib
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from langgraph.graph import END, StateGraph
from sqlalchemy import select
from typing_extensions import TypedDict

from app.agents.data_sources import DATA_SOURCES, count_historical_events, get_last_disaster_event
from app.agents.llm_provider import build_llm_runtime
from app.core.config import get_settings
from app.db import SessionLocal
from app.models import Region, Warning
from app.schemas import JiusiWarningResult, JiusiWorkflowOutput
from app.warning_filters import is_test_warning

settings = get_settings()
llm_runtime = build_llm_runtime(settings)

LEVELS = ["green", "yellow", "orange", "red"]


def _has_cjk(text: str) -> bool:
    for ch in str(text or ""):
        if "\u4e00" <= ch <= "\u9fff":
            return True
    return False


def _display_quality_note(note: str) -> str:
    """
    Keep user-facing notes concise and Chinese-only.
    We still keep full `source_status` + logs for debugging.
    """
    text = str(note or "").strip()
    if not text:
        return ""
    # Remove verbose internal error summary that looks noisy in UI.
    text = re.sub(r"(?:^|；)存在\\d+个关键数据源错误", "", text)
    # Normalize separators.
    text = re.sub(r"；{2,}", "；", text).strip("；").strip()
    return text


def _normalize_llm_reason_append_zh(text: str) -> str:
    """
    LLM sometimes returns English `reason_append`. Convert common patterns to Chinese;
    otherwise drop it to avoid English showing in UI.
    """
    raw = str(text or "").strip()
    if not raw:
        return ""
    if _has_cjk(raw):
        return raw

    lowered = raw.lower().replace("_", " ")
    parts = re.split(r"[;,，。\\n\\r]+", lowered)
    zh: List[str] = []
    for part in parts:
        p = part.strip()
        if not p:
            continue
        if "risk score" in p or "risk_score" in p:
            if "high" in p:
                zh.append("风险分偏高")
            elif "low" in p:
                zh.append("风险分偏低")
            else:
                zh.append("风险分异常")
            continue
        if "heavy rain" in p or ("rain" in p and "heavy" in p):
            zh.append("降雨偏强")
            continue
        if "rain" in p and ("increase" in p or "rising" in p):
            zh.append("降雨有上升趋势")
            continue
        if "history" in p and ("high" in p or "many" in p):
            zh.append("历史主动预警偏多")
            continue
        if "data quality" in p:
            m = re.search(r"(\\d+)", p)
            if m:
                zh.append(f"数据质量存在问题（{m.group(1)}项）")
            else:
                zh.append("数据质量存在问题")
            continue
        if "missing" in p and ("met" in p or "meteorology" in p):
            zh.append("气象数据存在缺失")
            continue
        if "missing" in p and ("geo" in p or "geology" in p):
            zh.append("地质数据存在缺失")
            continue

    # If we failed to translate, do not surface raw English to users.
    seen: set[str] = set()
    deduped: List[str] = []
    for item in zh:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return "，".join(deduped) if deduped else ""

class RegionInput(TypedDict):
    name: str
    code: str


class WarningState(TypedDict, total=False):
    timestamp: datetime
    regions: List[RegionInput]
    force_llm: bool
    collected_data: Dict[str, Any]
    previous_snapshot: Dict[str, Any]
    changed_regions: List[str]
    analysis: Dict[str, Any]
    llm_refinement: Dict[str, Any]
    decisions: Dict[str, Any]


def _clamp(v: float, low: float, high: float) -> float:
    return max(low, min(high, v))


def _level_from_score(score: float) -> str:
    if score < 25:
        return "green"
    if score < 50:
        return "yellow"
    if score < 75:
        return "orange"
    return "red"


def _distance_to_nearest_threshold(score: float) -> float:
    """
    Distance from risk score to nearest decision threshold (25/50/75), normalized to [0, 1].
    Closer to threshold means higher ambiguity and lower confidence.
    """
    score = _clamp(score, 0, 100)
    thresholds = [25.0, 50.0, 75.0]
    nearest = min(abs(score - t) for t in thresholds)
    return _clamp(nearest / 25.0, 0.0, 1.0)


def _baseline_score_from_region_code(region_code: str) -> float:
    # Conservative baseline when absolutely no live features are available.
    digest = hashlib.sha256(str(region_code).encode("utf-8")).digest()
    raw = int.from_bytes(digest[:2], byteorder="big", signed=False) / 65535.0
    return round(18 + raw * 28, 2)


def _has_numeric_features(payload: Dict[str, Any], keys: List[str]) -> bool:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, (int, float)):
            return True
    return False


def _score_midpoint_by_level(level: str | None) -> float:
    if level == "red":
        return 87.5
    if level == "orange":
        return 62.5
    if level == "yellow":
        return 37.5
    return 12.5


def _merge_channel_payload(channel: str, payloads: List[Dict[str, Any]]) -> Dict[str, Any]:
    numeric_sum: Dict[str, float] = {}
    numeric_weight: Dict[str, float] = {}
    non_numeric: Dict[str, Any] = {}

    for item in payloads:
        reliability = float(item.get("_reliability", 0.5))
        for key, value in item.items():
            if key.startswith("_"):
                continue
            if isinstance(value, (int, float)):
                numeric_sum[key] = numeric_sum.get(key, 0.0) + float(value) * reliability
                numeric_weight[key] = numeric_weight.get(key, 0.0) + reliability
            else:
                # Preserve first-seen non-numeric fields.
                # DATA_SOURCES is ordered by preference, so this avoids a low-reliability scraper
                # overriding CMA-simulated/live metadata such as `data_mode`.
                non_numeric.setdefault(key, value)

    merged: Dict[str, Any] = {}
    for key, total in numeric_sum.items():
        weight = max(0.001, numeric_weight.get(key, 1.0))
        merged[key] = round(total / weight, 3)
    merged.update(non_numeric)
    merged["_channel"] = channel
    return merged


def _parse_latest_warning_snapshot(region_code: str) -> Dict[str, Any]:
    db = SessionLocal()
    try:
        rows = (
            db.execute(
                select(Warning, Region)
                .join(Region, Region.id == Warning.region_id)
                .where(Region.code == region_code)
                .order_by(Warning.created_at.desc())
                .limit(30)
            )
            .all()
        )
        if not rows:
            return {}

        warning = None
        region = None
        for candidate_warning, candidate_region in rows:
            if is_test_warning(source=candidate_warning.source, reason=candidate_warning.reason):
                continue
            warning = candidate_warning
            region = candidate_region
            break

        if warning is None or region is None:
            return {}

        meteorology: Dict[str, Any] = {}
        if warning.meteorology:
            try:
                meteorology = json.loads(warning.meteorology)
            except Exception:
                meteorology = {}
        return {
            "region_name": region.name,
            "level": warning.level,
            "confidence": float(meteorology.get("confidence", 0.5)),
            "meteorology": meteorology,
            "created_at": warning.created_at.isoformat(),
        }
    finally:
        db.close()


def _change_score(current: Dict[str, Any], previous: Dict[str, Any]) -> tuple[float, List[str]]:
    changed_fields: List[str] = []
    score = 0.0
    keys = ["rain_24h", "rain_1h", "soil_moisture", "wind_speed", "slope", "fault_distance"]
    for key in keys:
        cv = current.get(key)
        pv = previous.get(key)
        if not isinstance(cv, (int, float)) or not isinstance(pv, (int, float)):
            continue
        base = max(1.0, abs(float(pv)))
        diff_ratio = abs(float(cv) - float(pv)) / base
        if diff_ratio > 0.12:
            changed_fields.append(key)
            score += min(0.35, diff_ratio)
    return _clamp(score, 0.0, 1.0), changed_fields


def _risk_score_from_data(data: Dict[str, Any]) -> tuple[float, str]:
    met = data.get("meteorology", {})
    geo = data.get("geology", {})
    hist = data.get("history", {})
    quality_note = str(data.get("data_quality_note", "") or "")

    source_status = data.get("source_status", {})
    success = source_status.get("success", {}) if isinstance(source_status, dict) else {}
    met_sources = success.get("meteorology", []) if isinstance(success, dict) else []
    geo_sources = success.get("geology", []) if isinstance(success, dict) else []
    has_met_sources = bool(met_sources)
    has_geo_sources = bool(geo_sources)
    reused_met = "气象源缺失，沿用上轮有效观测" in quality_note
    reused_geo = "地质源缺失，沿用上轮有效观测" in quality_note

    rain_24h_raw = met.get("rain_24h")
    rain_1h_raw = met.get("rain_1h")
    slope_raw = geo.get("slope")
    fault_raw = geo.get("fault_distance")

    rain_24h = float(rain_24h_raw or 0)
    rain_1h = float(rain_1h_raw or 0)
    soil_m = float(met.get("soil_moisture", 0) or 0)
    wind = float(met.get("wind_speed", 0) or 0)
    slope = float(slope_raw or 0)
    fault = float(fault_raw or 50)
    history_count = float(hist.get("landslides_count_10y", 0) or 0)

    rain_component = min(1.0, rain_24h / 120.0) * 35
    short_rain_component = min(1.0, rain_1h / 40.0) * 20
    soil_component = _clamp(soil_m, 0, 1) * 10
    wind_component = min(1.0, wind / 25.0) * 5
    slope_component = min(1.0, slope / 45.0) * 15
    fault_component = min(1.0, 2.0 / max(0.4, fault)) * 10
    history_component = min(1.0, history_count / 8.0) * 5

    total = (
        rain_component
        + short_rain_component
        + soil_component
        + wind_component
        + slope_component
        + fault_component
        + history_component
    )
    pieces: List[str] = []

    # Describe meteorology only when we have sources or we explicitly reused last valid observations.
    # This avoids showing synthetic 0s as if they were observed.
    if has_met_sources or reused_met:
        if "缺少24小时降雨" not in quality_note and isinstance(rain_24h_raw, (int, float)):
            pieces.append(f"24h降雨{rain_24h:.1f}mm")
        if isinstance(rain_1h_raw, (int, float)):
            pieces.append(f"1h降雨{rain_1h:.1f}mm")
        humidity_raw = met.get("humidity")
        wind_raw = met.get("wind_speed")
        if isinstance(humidity_raw, (int, float)) and float(humidity_raw) > 0:
            pieces.append(f"湿度{float(humidity_raw):.0f}%")
        if isinstance(wind_raw, (int, float)) and float(wind_raw) > 0:
            pieces.append(f"风速{float(wind_raw):.1f}m/s")

    # Geology fields are frequently unavailable before official API integration.
    # Hide them unless we have geology sources or explicitly reused last valid observations.
    if has_geo_sources or reused_geo:
        if isinstance(slope_raw, (int, float)) and float(slope_raw) > 0:
            pieces.append(f"坡度{slope:.1f}°")
        if isinstance(fault_raw, (int, float)) and float(fault_raw) > 0 and float(fault_raw) < 999:
            pieces.append(f"断层距离{fault:.1f}km")

    if history_count > 0:
        pieces.append(f"历史主动预警次数{int(history_count)}次")

    summary = ", ".join(pieces) if pieces else "关键观测项不足"
    return round(_clamp(total, 0, 100), 2), summary


def _infer_hazard_candidates(data: Dict[str, Any]) -> List[str]:
    met = data.get("meteorology", {})
    geo = data.get("geology", {})

    rain_24h = float(met.get("rain_24h", 0) or 0)
    rain_1h = float(met.get("rain_1h", 0) or 0)
    soil_m = float(met.get("soil_moisture", 0) or 0)
    slope = float(geo.get("slope", 0) or 0)
    fault = float(geo.get("fault_distance", 50) or 50)

    candidates: List[tuple[str, float]] = []

    landslide_score = 0.45 * min(1.0, slope / 45.0) + 0.35 * min(1.0, rain_24h / 120.0) + 0.2 * _clamp(soil_m, 0, 1)
    debris_flow_score = 0.5 * min(1.0, rain_1h / 40.0) + 0.3 * min(1.0, rain_24h / 120.0) + 0.2 * min(1.0, slope / 45.0)
    collapse_score = 0.6 * min(1.0, slope / 45.0) + 0.4 * min(1.0, 2.0 / max(0.4, fault))
    subsidence_score = 0.7 * min(1.0, 2.0 / max(0.4, fault)) + 0.3 * _clamp(soil_m, 0, 1)

    candidates.append(("滑坡", landslide_score))
    candidates.append(("泥石流", debris_flow_score))
    candidates.append(("崩塌", collapse_score))
    candidates.append(("地面塌陷", subsidence_score))

    sorted_candidates = sorted(candidates, key=lambda x: x[1], reverse=True)
    picked = [name for name, score in sorted_candidates if score >= 0.28][:3]
    if picked:
        return picked

    # Fallback: when geology fields are missing (common before official API),
    # strict thresholding produces an empty list. For user-facing explanations,
    # return the top-N candidates when there is any meteorological/geological signal.
    has_any_signal = (rain_24h > 0) or (rain_1h > 0) or (soil_m > 0) or (slope > 0) or (fault < 50)
    if has_any_signal:
        return [name for name, _score in sorted_candidates][:3]
    return []


def _adjacency(regions: List[RegionInput]) -> Dict[str, List[str]]:
    by_prefix: Dict[str, List[str]] = {}
    for region in regions:
        code = str(region["code"])
        prefix = code[:2]
        by_prefix.setdefault(prefix, []).append(code)

    graph: Dict[str, List[str]] = {}
    for region in regions:
        code = str(region["code"])
        neighbors = [c for c in by_prefix.get(code[:2], []) if c != code]
        graph[code] = neighbors
    return graph


async def coordinator_agent(state: WarningState) -> WarningState:
    state.setdefault("collected_data", {})
    state.setdefault("previous_snapshot", {})
    state.setdefault("changed_regions", [])
    state.setdefault("analysis", {})
    state.setdefault("llm_refinement", {})
    state.setdefault("decisions", {})
    state.setdefault("force_llm", True)
    return state


async def data_collector_agent(state: WarningState) -> WarningState:
    collected: Dict[str, Any] = {}
    previous_snapshot: Dict[str, Any] = {}
    regions = list(state.get("regions", []))
    max_concurrency = max(1, int(settings.collector_max_concurrency or 1))
    semaphore = asyncio.Semaphore(max_concurrency)

    async def _collect_single_source(
        *,
        source_name: str,
        source: Any,
        region_code: str,
        region_name: str,
    ) -> tuple[str, str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        try:
            try:
                raw = await source.fetch(region_code, region_name=region_name)
            except TypeError:
                # Backward compatibility for third-party sources registered with old signature.
                raw = await source.fetch(region_code)
            normalized = source.normalize(raw)
            if "error" in normalized:
                return source_name, str(getattr(source, "channel", "meteorology")), None, normalized
            payload = dict(normalized)
            payload["_source_name"] = source_name
            payload["_reliability"] = float(getattr(source, "reliability", 0.5))
            return source_name, str(getattr(source, "channel", "meteorology")), payload, None
        except Exception as exc:
            return (
                source_name,
                str(getattr(source, "channel", "meteorology")),
                None,
                {"error": str(exc)},
            )

    async def _collect_single_region(region: RegionInput) -> tuple[str, Dict[str, Any], Dict[str, Any]]:
        async with semaphore:
            code = region["code"]
            name = region["name"]
            try:
                channel_payloads: Dict[str, List[Dict[str, Any]]] = {"meteorology": [], "geology": []}
                channel_sources: Dict[str, List[str]] = {"meteorology": [], "geology": []}
                errors: Dict[str, Any] = {}

                source_results = await asyncio.gather(
                    *[
                        _collect_single_source(
                            source_name=source_name,
                            source=source,
                            region_code=code,
                            region_name=name,
                        )
                        for source_name, source in DATA_SOURCES.items()
                    ]
                )

                for source_name, channel, payload, error in source_results:
                    if error is not None:
                        errors[source_name] = error
                        continue
                    if payload is None:
                        continue
                    if channel in channel_payloads:
                        channel_payloads[channel].append(payload)
                        channel_sources[channel].append(source_name)

                history: Dict[str, Any] = {
                    "landslides_count_10y": await count_historical_events(code, years=10),
                    "last_event": await get_last_disaster_event(code),
                }

                meteorology = _merge_channel_payload("meteorology", channel_payloads["meteorology"])
                geology = _merge_channel_payload("geology", channel_payloads["geology"])

                previous = _parse_latest_warning_snapshot(code)

                prev_features: Dict[str, Any] = {}
                prev_met = previous.get("meteorology", {})
                if isinstance(prev_met, dict):
                    prev_features.update(prev_met)
                prev_features["slope"] = prev_met.get("slope", geology.get("slope", 0))
                prev_features["fault_distance"] = prev_met.get("fault_distance", geology.get("fault_distance", 999))

                current_features = {
                    "rain_24h": meteorology.get("rain_24h", 0),
                    "rain_1h": meteorology.get("rain_1h", 0),
                    "soil_moisture": meteorology.get("soil_moisture", 0),
                    "wind_speed": meteorology.get("wind_speed", 0),
                    "slope": geology.get("slope", 0),
                    "fault_distance": geology.get("fault_distance", 999),
                }
                change_score, changed_fields = _change_score(current_features, prev_features)

                region_payload = {
                    "region_name": name,
                    "region_code": code,
                    "meteorology": meteorology,
                    "geology": geology,
                    "history": history,
                    "source_errors": errors,
                    "source_status": {
                        "success": channel_sources,
                        "errors": errors,
                    },
                    "change_score": change_score,
                    "changed_fields": changed_fields,
                }
                return code, region_payload, previous
            except Exception as exc:
                fallback_payload = {
                    "region_name": name,
                    "region_code": code,
                    "meteorology": {},
                    "geology": {},
                    "history": {},
                    "source_errors": {"collector": {"error": str(exc)}},
                    "source_status": {"success": {"meteorology": [], "geology": []}, "errors": {"collector": {"error": str(exc)}}},
                    "change_score": 0.0,
                    "changed_fields": [],
                }
                return code, fallback_payload, {}

    results = await asyncio.gather(*[_collect_single_region(region) for region in regions])
    for code, region_payload, previous in results:
        collected[code] = region_payload
        previous_snapshot[code] = previous

    state["collected_data"] = collected
    state["previous_snapshot"] = previous_snapshot
    state["changed_regions"] = [
        code
        for code, data in collected.items()
        if data.get("change_score", 0) >= 0.12 or not previous_snapshot.get(code)
    ]
    return state


async def data_validation_agent(state: WarningState) -> WarningState:
    validated: Dict[str, Any] = {}

    for region_code, data in state.get("collected_data", {}).items():
        met = data.get("meteorology", {})
        geo = data.get("geology", {})
        previous = state.get("previous_snapshot", {}).get(region_code, {})
        prev_met = previous.get("meteorology", {}) if isinstance(previous, dict) else {}
        quality = 1.0
        notes: List[str] = []

        source_status = data.get("source_status", {})
        success = source_status.get("success", {}) if isinstance(source_status, dict) else {}
        met_sources = success.get("meteorology", []) if isinstance(success, dict) else []
        geo_sources = success.get("geology", []) if isinstance(success, dict) else []

        # Promote AMap precipitation estimates (rain_*_est) only when mm rain is otherwise missing.
        # This prevents heuristic estimates from overriding real precipitation from official/scraper sources.
        if met.get("rain_24h") is None and isinstance(met.get("rain_24h_est"), (int, float)):
            met["rain_24h"] = float(met.get("rain_24h_est") or 0.0)
            quality -= 0.06
            notes.append("降雨为估算（高德天气）")
        if met.get("rain_1h") is None and isinstance(met.get("rain_1h_est"), (int, float)):
            met["rain_1h"] = float(met.get("rain_1h_est") or 0.0)
            quality -= 0.04
            if "降雨为估算（高德天气）" not in notes:
                notes.append("降雨为估算（高德天气）")

        # If AMap provided an estimation note, keep it for UI/debugging.
        if met.get("precipitation_note") and "降雨为估算（高德天气）" in notes:
            met.setdefault("data_quality_hint", str(met.get("precipitation_note")))

        # If this round has no successful source in a channel, reuse previous valid values
        # to avoid replacing real risk with artificial low values.
        if not met_sources and isinstance(prev_met, dict):
            for key in ["rain_24h", "rain_1h", "soil_moisture", "wind_speed", "humidity"]:
                pv = prev_met.get(key)
                cv = met.get(key)
                if isinstance(pv, (int, float)) and (
                    cv is None
                    or (isinstance(cv, (int, float)) and float(cv) <= 0)
                ):
                    met[key] = float(pv)
            if any(isinstance(prev_met.get(k), (int, float)) for k in ["rain_24h", "rain_1h", "wind_speed", "humidity"]):
                quality -= 0.08
                notes.append("气象源缺失，沿用上轮有效观测")

        if not geo_sources and isinstance(prev_met, dict):
            for key in ["slope", "fault_distance"]:
                pv = prev_met.get(key)
                cv = geo.get(key)
                if isinstance(pv, (int, float)) and (
                    cv is None
                    or (isinstance(cv, (int, float)) and float(cv) <= 0)
                ):
                    geo[key] = float(pv)
            if prev_met.get("lithology") and not geo.get("lithology"):
                geo["lithology"] = prev_met.get("lithology")
            if any(isinstance(geo.get(k), (int, float)) for k in ["slope", "fault_distance"]):
                quality -= 0.06
                notes.append("地质源缺失，沿用上轮有效观测")

        if met.get("rain_24h") is None:
            met["rain_24h"] = 0
            quality -= 0.15
            notes.append("缺少24小时降雨")
        if float(met.get("rain_24h", 0) or 0) < 0:
            met["rain_24h"] = 0
            quality -= 0.1
            notes.append("24小时降雨异常")
        if float(met.get("rain_24h", 0) or 0) > 600:
            quality -= 0.2
            notes.append("24小时降雨疑似异常")

        if float(geo.get("slope", 0) or 0) < 0:
            geo["slope"] = 0
            quality -= 0.1
            notes.append("坡度异常")

        if data.get("source_errors"):
            benign_errors = {
                "scraper_disabled",
                "geology_scraper_disabled",
                "scraper_template_not_configured",
                "geology_scraper_template_not_configured",
                "invalid_scraper_template",
                "unsupported_scraper_payload",
                "scraper_parser_disabled_temporarily",
                "scraper_budget_exceeded",
                "missing_cma_api_key",
                "missing_cma_credentials",
                "missing_amap_api_key",
                "missing_wu_api_key",
                "missing_cgs_api_key",
                "missing_openweather_api_key",
                "coordinates_required",
                "wu_disabled",
                "wu_no_region_coordinates",
                "wu_key_rejected",
                "domain_not_allowed",
                "government_domain_blocked",
                "tianqi_slug_not_found",
                "amap_parse_failed",
                "cma_station_not_mapped",
                "cma_parse_failed",
            }
            severe_errors = 0
            for err in data["source_errors"].values():
                error_code = str((err or {}).get("error", "")).strip()
                if error_code and error_code not in benign_errors:
                    severe_errors += 1
            if severe_errors > 0:
                quality -= min(0.25, 0.05 * severe_errors)
                notes.append(f"存在{severe_errors}个关键数据源错误")

        data["meteorology"] = met
        data["geology"] = geo
        data["data_quality_score"] = round(_clamp(quality, 0.2, 1.0), 3)
        data["data_quality_note"] = "；".join(notes) if notes else "数据完整"
        validated[region_code] = data

    state["collected_data"] = validated
    return state


async def local_risk_agent(state: WarningState) -> WarningState:
    analysis: Dict[str, Any] = {}

    for code, data in state.get("collected_data", {}).items():
        quality_score = float(data.get("data_quality_score", 0.6))
        change_score = float(data.get("change_score", 0))
        previous = state.get("previous_snapshot", {}).get(code, {})
        met_payload = data.get("meteorology", {}) if isinstance(data.get("meteorology"), dict) else {}
        geo_payload = data.get("geology", {}) if isinstance(data.get("geology"), dict) else {}
        has_met = _has_numeric_features(met_payload, ["rain_24h", "rain_1h", "soil_moisture", "wind_speed"])
        has_geo = _has_numeric_features(geo_payload, ["slope", "fault_distance"])

        if not has_met and not has_geo:
            if previous:
                prev_level = str(previous.get("level", "green") or "green")
                prev_conf = float(previous.get("confidence", 0.5) or 0.5)
                fallback_conf = _clamp(prev_conf - 0.12, 0.25, 0.88)
                analysis[code] = {
                    "region_name": data.get("region_name"),
                    "region_code": code,
                    "level": prev_level if prev_level in LEVELS else "green",
                    "risk_score": _score_midpoint_by_level(prev_level),
                    "confidence": round(fallback_conf, 3),
                    "factors": "本轮未获取到有效气象/地质数据，沿用上次预警并下调置信度。",
                    "changed_fields": data.get("changed_fields", []),
                    "change_score": round(change_score, 3),
                    "data_quality_note": "数据缺失，采用上次结果兜底。",
                    "hazard_candidates": [],
                    "confidence_breakdown": {
                        "formula": "兜底策略：沿用上次预警并降低置信度",
                        "quality_score": round(quality_score, 3),
                        "change_score": round(_clamp(change_score, 0.0, 1.0), 3),
                        "source_coverage": 0.0,
                        "threshold_distance": 0.0,
                        "stability_bonus": -0.12,
                        "neighbor_bonus": 0.0,
                        "llm_delta": 0.0,
                        "raw_confidence_before_neighbor": round(fallback_conf, 3),
                    },
                    "confidence_reason": "本轮缺少有效观测，沿用上次预警并下调置信度。",
                }
                continue

            baseline_score = _baseline_score_from_region_code(code)
            level = _level_from_score(baseline_score)
            analysis[code] = {
                "region_name": data.get("region_name"),
                "region_code": code,
                "level": level,
                "risk_score": baseline_score,
                "confidence": 0.3,
                "factors": "本轮无有效外部观测，采用保守地区基线估算。",
                "changed_fields": data.get("changed_fields", []),
                "change_score": round(change_score, 3),
                "data_quality_note": "数据缺失，采用保守基线。",
                "hazard_candidates": [],
                "confidence_breakdown": {
                    "formula": "兜底策略：地区基线估算",
                    "quality_score": round(quality_score, 3),
                    "change_score": round(_clamp(change_score, 0.0, 1.0), 3),
                    "source_coverage": 0.0,
                    "threshold_distance": 0.0,
                    "stability_bonus": -0.2,
                    "neighbor_bonus": 0.0,
                    "llm_delta": 0.0,
                    "raw_confidence_before_neighbor": 0.3,
                },
                "confidence_reason": "本轮缺少有效观测，使用保守地区基线，置信度较低。",
            }
            continue

        risk_score, summary = _risk_score_from_data(data)
        level = _level_from_score(risk_score)
        source_status = data.get("source_status", {})
        success = source_status.get("success", {}) if isinstance(source_status, dict) else {}
        met_sources = success.get("meteorology", []) if isinstance(success, dict) else []
        geo_sources = success.get("geology", []) if isinstance(success, dict) else []
        source_coverage = 0.0
        if met_sources:
            source_coverage += 0.5
        if geo_sources:
            source_coverage += 0.5

        threshold_distance = _distance_to_nearest_threshold(risk_score)
        stability_bonus = 0.0
        if previous:
            prev_level = str(previous.get("level", "")).strip()
            if prev_level and prev_level == level:
                stability_bonus = 0.03
            elif prev_level and prev_level != level:
                stability_bonus = -0.02
        else:
            stability_bonus = -0.04

        data_mode_notes: List[str] = []
        if str(data.get("meteorology", {}).get("data_mode", "")).lower() == "simulated":
            data_mode_notes.append("气象为模拟数据")
        if str(data.get("geology", {}).get("data_mode", "")).lower() == "simulated":
            data_mode_notes.append("地质为模拟数据")
        mode_note = f"；{'；'.join(data_mode_notes)}" if data_mode_notes else ""

        confidence = (
            0.36
            + 0.30 * quality_score
            + 0.16 * _clamp(change_score, 0.0, 1.0)
            + 0.14 * source_coverage
            + 0.10 * threshold_distance
            + stability_bonus
        )
        confidence = _clamp(confidence, 0.30, 0.96)
        hazard_candidates = _infer_hazard_candidates(data)

        confidence_breakdown = {
            "formula": "0.36 + 0.30*质量分 + 0.16*变化分 + 0.14*数据源覆盖 + 0.10*阈值距离 + 稳定性修正 + 邻区修正 + LLM修正",
            "quality_score": round(quality_score, 3),
            "change_score": round(_clamp(change_score, 0.0, 1.0), 3),
            "source_coverage": round(source_coverage, 3),
            "threshold_distance": round(threshold_distance, 3),
            "stability_bonus": round(stability_bonus, 3),
            "neighbor_bonus": 0.0,
            "llm_delta": 0.0,
            "raw_confidence_before_neighbor": round(confidence, 3),
        }
        confidence_reason = (
            f"质量分{quality_score:.2f}，变化分{change_score:.2f}，数据源覆盖{source_coverage:.2f}，"
            f"阈值距离{threshold_distance:.2f}，稳定性修正{stability_bonus:+.2f}。"
        )

        analysis[code] = {
            "region_name": data.get("region_name"),
            "region_code": code,
            "level": level,
            "risk_score": risk_score,
            "confidence": round(confidence, 3),
            "factors": f"{summary}。{_display_quality_note(str(data.get('data_quality_note', '') or '')) or '数据完整'}{mode_note}",
            "changed_fields": data.get("changed_fields", []),
            "change_score": round(change_score, 3),
            "data_quality_note": data.get("data_quality_note", ""),
            "hazard_candidates": hazard_candidates,
            "confidence_breakdown": confidence_breakdown,
            "confidence_reason": confidence_reason,
        }

    state["analysis"] = analysis
    return state


async def neighbor_influence_agent(state: WarningState) -> WarningState:
    analysis = state.get("analysis", {})
    adjacency = _adjacency(state.get("regions", []))
    weight = _clamp(float(settings.neighbor_influence_weight), 0.0, 0.5)

    for code, item in analysis.items():
        neighbors = adjacency.get(code, [])
        if not neighbors:
            continue
        neighbor_scores = [
            float(analysis[n].get("risk_score", 0)) for n in neighbors if n in analysis
        ]
        if not neighbor_scores:
            continue

        local_score = float(item.get("risk_score", 0))
        neighbor_avg = sum(neighbor_scores) / len(neighbor_scores)
        adjusted = (1 - weight) * local_score + weight * neighbor_avg

        item["risk_score"] = round(adjusted, 2)
        item["level"] = _level_from_score(adjusted)
        neighbor_bonus = 0.03
        item["confidence"] = round(_clamp(float(item.get("confidence", 0.5)) + neighbor_bonus, 0, 1), 3)
        item["factors"] = (
            f"{item.get('factors', '')}；邻区影响均值{neighbor_avg:.1f}，融合后风险分{adjusted:.1f}"
        )
        breakdown = item.get("confidence_breakdown", {})
        if isinstance(breakdown, dict):
            breakdown["neighbor_bonus"] = round(neighbor_bonus, 3)
            item["confidence_breakdown"] = breakdown
        item["confidence_reason"] = (
            f"{item.get('confidence_reason', '')} 邻区修正{neighbor_bonus:+.2f}。".strip()
        )

    state["analysis"] = analysis
    return state


async def llm_refinement_agent(state: WarningState) -> WarningState:
    analysis = state.get("analysis", {})
    collected = state.get("collected_data", {})
    changed = set(state.get("changed_regions", []))
    force_llm = bool(state.get("force_llm", True))

    if not llm_runtime:
        state["llm_refinement"] = {}
        return state
    if not settings.enable_llm_refinement and not force_llm:
        state["llm_refinement"] = {}
        return state

    candidates: List[str] = []
    for code, item in analysis.items():
        confidence = float(item.get("confidence", 0.5))
        if code in changed or confidence < float(settings.llm_confidence_threshold):
            candidates.append(code)

    if force_llm and not candidates:
        sorted_codes = sorted(
            analysis.keys(),
            key=lambda c: float(analysis[c].get("risk_score", 0)),
            reverse=True,
        )
        if sorted_codes:
            candidates = [sorted_codes[0]]

    candidates = candidates[: max(1, int(settings.llm_refine_max_regions))]
    if not candidates:
        state["llm_refinement"] = {}
        return state

    payload = []
    for code in candidates:
        item = analysis[code]
        row = collected.get(code, {})
        payload.append(
            {
                "region_name": item.get("region_name"),
                "region_code": code,
                "current_level": item.get("level"),
                "current_confidence": item.get("confidence"),
                "risk_score": item.get("risk_score"),
                "change_score": item.get("change_score"),
                "changed_fields": item.get("changed_fields"),
                "meteorology": {
                    "rain_24h": row.get("meteorology", {}).get("rain_24h"),
                    "rain_1h": row.get("meteorology", {}).get("rain_1h"),
                    "soil_moisture": row.get("meteorology", {}).get("soil_moisture"),
                    "wind_speed": row.get("meteorology", {}).get("wind_speed"),
                },
                "geology": {
                    "slope": row.get("geology", {}).get("slope"),
                    "fault_distance": row.get("geology", {}).get("fault_distance"),
                },
                "history_count": row.get("history", {}).get("landslides_count_10y"),
                "data_quality_note": row.get("data_quality_note"),
            }
        )

    system_prompt = (
        "你是地质灾害预警复核助手。请对给定地区做轻量复核，只在必要时调整等级。"
        "输出JSON: {\"results\":[{\"region_code\":str,\"level\":\"green|yellow|orange|red\","
        "\"confidence_delta\":float,\"reason_append\":str}]}"
        "其中confidence_delta范围[-0.15,0.15]。reason_append 必须为中文、简短、可解释（不超过30字）。不要输出其他文字。"
    )
    user_prompt = json.dumps(
        {
            "timestamp": state.get("timestamp").isoformat() if state.get("timestamp") else None,
            "provider": llm_runtime.provider,
            "items": payload,
        },
        ensure_ascii=False,
    )

    try:
        resp = await llm_runtime.client.chat.completions.create(
            model=llm_runtime.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=max(180, 60 * len(payload)),
        )
        parsed = json.loads(resp.choices[0].message.content or "{}")
    except Exception:
        parsed = {"results": []}

    refinement: Dict[str, Any] = {}
    for row in parsed.get("results", []):
        code = str(row.get("region_code", "")).strip()
        if not code:
            continue
        level = row.get("level")
        if level not in LEVELS:
            level = None
        refinement[code] = {
            "level": level,
            "confidence_delta": _clamp(float(row.get("confidence_delta", 0)), -0.15, 0.15),
            "reason_append": _normalize_llm_reason_append_zh(str(row.get("reason_append", "")).strip()),
            "provider": llm_runtime.provider,
        }

    state["llm_refinement"] = refinement
    return state


async def decision_maker_agent(state: WarningState) -> WarningState:
    analysis = state.get("analysis", {})
    refinement = state.get("llm_refinement", {})
    collected = state.get("collected_data", {})

    decisions: Dict[str, Any] = {}
    for code, item in analysis.items():
        level = item.get("level", "green")
        confidence = float(item.get("confidence", 0.5))
        reason = str(item.get("factors", ""))
        confidence_breakdown = dict(item.get("confidence_breakdown", {}))
        confidence_reason = str(item.get("confidence_reason", "")).strip()
        hazard_candidates = list(item.get("hazard_candidates", []))
        llm_delta = 0.0

        llm_item = refinement.get(code)
        if llm_item:
            if llm_item.get("level") in LEVELS:
                level = llm_item["level"]
            llm_delta = float(llm_item.get("confidence_delta", 0))
            confidence = _clamp(confidence + llm_delta, 0, 1)
            if llm_item.get("reason_append"):
                reason = f"{reason}；LLM复核：{llm_item['reason_append']}"

        met = dict(collected.get(code, {}).get("meteorology", {}))
        # Persist key geology features into the stored "meteorology" JSON so
        # future runs can reuse them when geology sources are temporarily missing.
        geo_payload = dict(collected.get(code, {}).get("geology", {}))
        for k in ["slope", "fault_distance", "lithology"]:
            v = geo_payload.get(k)
            if v in (None, "", "unknown"):
                continue
            if isinstance(v, (int, float)) and float(v) <= 0:
                continue
            met.setdefault(k, v)
        met["confidence"] = round(confidence, 3)
        met["risk_score"] = item.get("risk_score")
        met["changed_fields"] = item.get("changed_fields", [])
        met["data_quality_note"] = item.get("data_quality_note", "")
        met["source_status"] = collected.get(code, {}).get("source_status", {})
        if confidence_breakdown:
            confidence_breakdown["llm_delta"] = round(llm_delta, 3)
            confidence_breakdown["final_confidence"] = round(confidence, 3)
            met["confidence_breakdown"] = confidence_breakdown
        if confidence_reason:
            met["confidence_reason"] = confidence_reason + f" LLM修正{llm_delta:+.2f}。"
        # Always include hazard candidates when available (even if derived via fallback)
        # so the frontend can display them consistently.
        if hazard_candidates:
            met["hazard_candidates"] = hazard_candidates

        if hazard_candidates:
            reason = f"{reason}；最可能灾害：{'、'.join(hazard_candidates)}"

        decisions[code] = {
            "region_name": item.get("region_name"),
            "region_code": code,
            "level": level,
            "reason": reason,
            "confidence": round(confidence, 3),
            "meteorology": met,
        }

    state["decisions"] = decisions
    return state


def build_graph() -> Any:
    graph = StateGraph(WarningState)
    graph.add_node("coordinator", coordinator_agent)
    graph.add_node("data_collector", data_collector_agent)
    graph.add_node("data_validation", data_validation_agent)
    graph.add_node("local_risk", local_risk_agent)
    graph.add_node("neighbor_influence", neighbor_influence_agent)
    graph.add_node("llm_refinement", llm_refinement_agent)
    graph.add_node("decision_maker", decision_maker_agent)

    graph.set_entry_point("coordinator")
    graph.add_edge("coordinator", "data_collector")
    graph.add_edge("data_collector", "data_validation")
    graph.add_edge("data_validation", "local_risk")
    graph.add_edge("local_risk", "neighbor_influence")
    graph.add_edge("neighbor_influence", "llm_refinement")
    graph.add_edge("llm_refinement", "decision_maker")
    graph.add_edge("decision_maker", END)
    return graph.compile()


agraph = build_graph()


async def run_warning_workflow(
    *, timestamp: datetime, regions: List[RegionInput], force_llm: bool = True
) -> JiusiWorkflowOutput:
    initial_state: WarningState = {
        "timestamp": timestamp,
        "regions": regions,
        "force_llm": force_llm,
    }
    final_state = await agraph.ainvoke(initial_state)

    results: List[JiusiWarningResult] = []
    for code, item in final_state.get("decisions", {}).items():
        results.append(
            JiusiWarningResult(
                region_name=item.get("region_name"),
                region_code=item.get("region_code", code),
                level=item.get("level", "green"),
                reason=item.get("reason", ""),
                confidence=float(item.get("confidence", 0.5)),
                meteorology=item.get("meteorology") or {},
            )
        )

    return JiusiWorkflowOutput(timestamp=timestamp, results=results)



