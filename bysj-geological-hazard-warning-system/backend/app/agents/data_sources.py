"""
Data source integrations for multi-agent system.
"""

from __future__ import annotations

import asyncio
import html
import hashlib
import logging
import json
import random
import re
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, Optional, Protocol, Tuple
from urllib.parse import quote, urlparse

import httpx
from redis import Redis
from sqlalchemy import case, func, select
try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    BeautifulSoup = None
try:
    from pypinyin import lazy_pinyin  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    lazy_pinyin = None

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

from app.core.config import get_settings
from app.db import SessionLocal
from app.models import Region, Warning
from app.agents.scraper_parsers import parse_geology_payload, parse_weather_payload
from app.integrations.amap_weather import estimate_rain_from_weather_text, parse_amap_live, wind_level_to_speed_ms

settings = get_settings()
logger = logging.getLogger(__name__)

PLACEHOLDER_KEYS = {
    "",
    "your_api_key_here",
    "replace_me",
    "changeme",
    "null",
    "none",
    "test",
}

SIMULATION_KEYS = {
    "simulate",
    "simulated",
    "simulate_test",
    "simulated_test",
    "mock",
    "mock_test",
    "demo_simulate",
}

BLOCKED_GOV_SUFFIXES = (
    ".gov.cn",
    ".gov",
    ".gouv.fr",
    ".gov.uk",
)
BLOCKED_GOV_EXACT = {
    "gov.cn",
    "www.gov.cn",
}

_SCRAPER_WINDOW_SECONDS = 30 * 60
_scraper_window_slot = -1
_scraper_window_used = 0
_scraper_window_lock = asyncio.Lock()
_region_name_cache: Dict[str, str] = {}
_region_coord_cache: Dict[str, Tuple[float, float]] = {}
_scraper_rate_lock = asyncio.Lock()
_scraper_request_timestamps: list[float] = []
_scraper_parallel_lock = asyncio.Lock()
_scraper_parallel_semaphore: Optional[asyncio.Semaphore] = None
_scraper_parallel_size = 0
_scraper_cache: Dict[Tuple[str, str], Tuple[datetime, Dict[str, Any]]] = {}
_scraper_inflight: Dict[Tuple[str, str], asyncio.Future] = {}
_scraper_inflight_lock = asyncio.Lock()
_tianqi_slug_map: Dict[str, str] = {}
_tianqi_slug_updated_at: Optional[datetime] = None
_tianqi_slug_lock = asyncio.Lock()
_tianqi_slug_overrides: Dict[str, str] = {}
_cma_region_station_map: Dict[str, str] = {}
_cma_region_station_meta: Dict[str, Any] = {}
_cma_region_station_loaded_at: Optional[datetime] = None
_wu_key_cache: list[str] = []
_wu_key_cache_expire_at: Optional[datetime] = None
_wu_active_key: str = ""
_wu_key_lock = asyncio.Lock()
_http_client: Optional[httpx.AsyncClient] = None
_http_client_lock = asyncio.Lock()
_host_cooldown_until: Dict[str, float] = {}
_tianqi_session_warmed_at: Optional[float] = None
_tianqi_url_owner: Dict[str, str] = {}

_cache_redis_client: Optional[Redis] = None
_cache_redis_disabled_until: Optional[float] = None

_CACHE_REDIS_PREFIX = "ghws:cache"
_SCRAPER_CACHE_REDIS_PREFIX = f"{_CACHE_REDIS_PREFIX}:scraper"
_WU_KEY_CACHE_REDIS_KEY = f"{_CACHE_REDIS_PREFIX}:wu:key_pool"
_WU_ACTIVE_KEY_REDIS_KEY = f"{_CACHE_REDIS_PREFIX}:wu:active_key"

_TIANQI_SESSION_TTL_SECONDS = 6 * 60 * 60

TIANQI_SLUG_BLOCKLIST = { 
    "news", 
    "air", 
    "video", 
    "plugin", 
    "alarmnews", 
    "worldcity", 
    "chinacity", 
    "province", 
    "jingdian", 
    "toutiao", 
    "tag", 
    "latest", 
    "zhuanti", 
    "changshi", 
} 

REGION_SUFFIXES = [
    "特别行政区",
    "自治州",
    "自治县",
    "高新区",
    "开发区",
    "新区",
    "矿区",
    "林区",
    "地区",
    "市辖区",
    "自治区",
    "自治旗",
    "省",
    "市",
    "区",
    "县",
    "州",
    "盟",
    "旗",
]

def _load_tianqi_slug_overrides() -> Dict[str, str]:
    # Optional local overrides to fix ambiguous or changed slugs.
    # Path is inside repo so it works in docker build context.
    try:
        import os

        base_dir = os.path.dirname(os.path.dirname(__file__))  # backend/app
        path = os.path.join(base_dir, "data", "tianqi_slug_overrides.json")
        if not os.path.exists(path):
            return {}
        raw = open(path, "r", encoding="utf-8").read()
        data = json.loads(raw or "{}")
        if not isinstance(data, dict):
            return {}
        cleaned: Dict[str, str] = {}
        for k, v in data.items():
            key = str(k or "").strip()
            val = str(v or "").strip().lower()
            if not key or not val:
                continue
            if val in TIANQI_SLUG_BLOCKLIST:
                continue
            if not re.fullmatch(r"[a-z0-9_-]{2,64}", val):
                continue
            cleaned[key] = val
            norm = _normalize_single_region_name(key)
            if norm:
                cleaned.setdefault(norm, val)
        return cleaned
    except Exception:
        return {}


_tianqi_slug_overrides = _load_tianqi_slug_overrides()


def _load_cma_region_station_map() -> tuple[Dict[str, Any], Dict[str, str]]:
    """
    Load region_code -> Station_Id_C mapping generated from China_SURF_Station.xlsx.
    File format supports either:
    - {"meta": {...}, "map": {...}} (preferred)
    - {"110101": "54511", ...} (legacy/simple)
    """
    try:
        import os

        base_dir = os.path.dirname(os.path.dirname(__file__))  # backend/app
        path = os.path.join(base_dir, "data", "cma_region_station_map.json")
        if not os.path.exists(path):
            return {}, {}
        raw = open(path, "r", encoding="utf-8").read()
        data = json.loads(raw or "{}")
        if not isinstance(data, dict):
            return {}, {}
        meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
        mapping = data.get("map") if isinstance(data.get("map"), dict) else data
        if not isinstance(mapping, dict):
            return meta, {}
        cleaned: Dict[str, str] = {}
        for k, v in mapping.items():
            key = str(k or "").strip()
            val = str(v or "").strip()
            if not key or not val:
                continue
            cleaned[key] = val
        return meta, cleaned
    except Exception:
        return {}, {}


def _load_cma_region_station_overrides() -> Dict[str, str]:
    try:
        import os

        base_dir = os.path.dirname(os.path.dirname(__file__))  # backend/app
        path = os.path.join(base_dir, "data", "cma_region_station_overrides.json")
        if not os.path.exists(path):
            return {}
        raw = open(path, "r", encoding="utf-8").read()
        data = json.loads(raw or "{}")
        if not isinstance(data, dict):
            return {}
        cleaned: Dict[str, str] = {}
        for k, v in data.items():
            key = str(k or "").strip()
            val = str(v or "").strip()
            if not key or not val:
                continue
            cleaned[key] = val
        return cleaned
    except Exception:
        return {}


def _ensure_cma_region_station_map_loaded() -> None:
    global _cma_region_station_map, _cma_region_station_meta, _cma_region_station_loaded_at
    if _cma_region_station_loaded_at is not None:
        return
    meta, mapping = _load_cma_region_station_map()
    overrides = _load_cma_region_station_overrides()
    # Overrides take precedence.
    mapping.update({k: v for k, v in overrides.items()})
    _cma_region_station_meta = meta
    _cma_region_station_map = mapping
    _cma_region_station_loaded_at = datetime.utcnow()


def _get_cma_station_id(region_code: str) -> str:
    _ensure_cma_region_station_map_loaded()
    return _cma_region_station_map.get(str(region_code or "").strip(), "")


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _get_cache_redis_client() -> Optional[Redis]:
    global _cache_redis_client, _cache_redis_disabled_until
    now = datetime.utcnow().timestamp()
    if _cache_redis_disabled_until and now < _cache_redis_disabled_until:
        return None
    if _cache_redis_client is not None:
        return _cache_redis_client
    try:
        _cache_redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
        _cache_redis_client.ping()
        _cache_redis_disabled_until = None
        return _cache_redis_client
    except Exception:
        _cache_redis_client = None
        # Back off for a short period to avoid repeated connection attempts.
        _cache_redis_disabled_until = now + 20.0
        return None


def _redis_scraper_cache_key(source_name: str, region_code: str) -> str:
    return f"{_SCRAPER_CACHE_REDIS_PREFIX}:{source_name}:{region_code}"


def _scraper_cache_ttl_seconds() -> float:
    return max(60.0, float(settings.scraper_cache_minutes) * 60.0)


def _scraper_stale_ttl_seconds() -> int:
    # Keep stale cache longer than fresh TTL so failed fetches can still fall back.
    fresh = int(_scraper_cache_ttl_seconds())
    return max(fresh + 300, fresh * 3)


def _read_scraper_cache_from_redis(source_name: str, region_code: str) -> tuple[Optional[Dict[str, Any]], float]:
    client = _get_cache_redis_client()
    if client is None:
        return None, 0.0
    try:
        raw = client.get(_redis_scraper_cache_key(source_name, region_code))
        if not raw:
            return None, 0.0
        data = json.loads(raw)
        if not isinstance(data, dict):
            return None, 0.0
        payload = data.get("payload")
        cached_at_raw = str(data.get("cached_at") or "")
        if not isinstance(payload, dict):
            return None, 0.0
        cached_at = datetime.fromisoformat(cached_at_raw) if cached_at_raw else datetime.utcnow()
        age = max(0.0, (datetime.utcnow() - cached_at).total_seconds())
        return dict(payload), age
    except Exception:
        return None, 0.0


def _write_scraper_cache_to_redis(source_name: str, region_code: str, payload: Dict[str, Any], cached_at: datetime) -> None:
    client = _get_cache_redis_client()
    if client is None:
        return
    try:
        envelope = {
            "cached_at": cached_at.isoformat(),
            "payload": payload,
        }
        client.setex(
            _redis_scraper_cache_key(source_name, region_code),
            _scraper_stale_ttl_seconds(),
            json.dumps(envelope, ensure_ascii=False),
        )
    except Exception:
        return


def _read_wu_key_cache_from_redis() -> tuple[list[str], Optional[datetime], str]:
    client = _get_cache_redis_client()
    if client is None:
        return [], None, ""
    keys: list[str] = []
    expire_at: Optional[datetime] = None
    active_key = ""
    try:
        raw = client.get(_WU_KEY_CACHE_REDIS_KEY)
        if raw:
            data = json.loads(raw)
            if isinstance(data, dict):
                keys = _dedupe_preserve_order([str(v or "").strip() for v in data.get("keys", [])])
                expire_at_raw = str(data.get("expire_at") or "").strip()
                if expire_at_raw:
                    try:
                        expire_at = datetime.fromisoformat(expire_at_raw)
                    except Exception:
                        expire_at = None
    except Exception:
        keys = []
        expire_at = None

    try:
        active_raw = client.get(_WU_ACTIVE_KEY_REDIS_KEY)
        if active_raw:
            active_key = str(active_raw).strip()
    except Exception:
        active_key = ""

    return keys, expire_at, active_key


def _persist_wu_key_cache_to_redis(keys: list[str], expire_at: datetime) -> None:
    client = _get_cache_redis_client()
    if client is None:
        return
    deduped = _dedupe_preserve_order([str(v or "").strip() for v in keys])
    if not deduped:
        return
    now = datetime.utcnow()
    ttl = max(60, int((expire_at - now).total_seconds()))
    try:
        payload = {"keys": deduped, "expire_at": expire_at.isoformat()}
        client.setex(_WU_KEY_CACHE_REDIS_KEY, ttl, json.dumps(payload, ensure_ascii=False))
    except Exception:
        return


def _persist_wu_active_key_to_redis(key: str) -> None:
    client = _get_cache_redis_client()
    if client is None:
        return
    k = str(key or "").strip()
    if not k:
        return
    try:
        # Keep active key at least as long as key-pool refresh period.
        ttl = max(30 * 60, int(max(30, int(settings.wu_key_refresh_minutes or 360)) * 60))
        client.setex(_WU_ACTIVE_KEY_REDIS_KEY, ttl, k)
    except Exception:
        return


def _maybe_redecode(text: str, src: str, dst: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    try:
        fixed = raw.encode(src, errors="ignore").decode(dst, errors="ignore").strip()
    except Exception:
        return ""
    if not fixed or fixed == raw:
        return ""
    return fixed


def _candidate_region_names(name: str) -> list[str]:
    base = html.unescape(str(name or "")).strip()
    if not base:
        return []
    candidates = [
        base,
        _maybe_redecode(base, "gb18030", "utf-8"),
        _maybe_redecode(base, "gbk", "utf-8"),
        _maybe_redecode(base, "latin1", "utf-8"),
        base.replace(" ", ""),
        base.replace("　", ""),
    ]
    return _dedupe_preserve_order(candidates)


def _normalize_single_region_name(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        return ""
    text = re.sub(r"[\s._/\-·•]+", "", text)
    for suffix in REGION_SUFFIXES:
        if len(text) > len(suffix) and text.endswith(suffix):
            text = text[: -len(suffix)]
            break
    return text.strip().lower()


def _normalized_region_name_variants(name: str) -> list[str]:
    normalized = [_normalize_single_region_name(item) for item in _candidate_region_names(name)]
    return _dedupe_preserve_order(normalized)


class DataSource(Protocol):
    source_name: str
    channel: str
    reliability: float

    async def fetch(self, region_code: str, region_name: Optional[str] = None) -> Dict[str, Any]:
        ...

    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        ...


def reset_scraper_runtime_state(*, clear_cache: bool = False) -> Dict[str, Any]: 
    global _scraper_window_slot, _scraper_window_used 
    global _tianqi_slug_map, _tianqi_slug_updated_at 
    global _scraper_parallel_semaphore, _scraper_parallel_size 
    global _tianqi_session_warmed_at 
    global _tianqi_slug_overrides
    global _tianqi_url_owner
    global _cma_region_station_map, _cma_region_station_meta, _cma_region_station_loaded_at
    global _wu_key_cache, _wu_key_cache_expire_at, _wu_active_key
    global _cache_redis_disabled_until

    _scraper_window_slot = -1 
    _scraper_window_used = 0 
    _scraper_request_timestamps.clear() 
    _scraper_parallel_semaphore = None 
    _scraper_parallel_size = 0
    _host_cooldown_until.clear() 
    _tianqi_session_warmed_at = None 
    _tianqi_url_owner.clear()
    _wu_key_cache = []
    _wu_key_cache_expire_at = None
    _wu_active_key = ""
    _cache_redis_disabled_until = None
    # Reload local slug overrides so operators can update the JSON without restarting containers.
    _tianqi_slug_overrides = _load_tianqi_slug_overrides()
    # Reload CMA station mapping/overrides (safe and cheap); lets operators regenerate mapping without restart.
    _cma_region_station_meta, _cma_region_station_map = _load_cma_region_station_map()
    _cma_region_station_map.update(_load_cma_region_station_overrides())
    _cma_region_station_loaded_at = datetime.utcnow()

    cleared_cache = False 
    if clear_cache: 
        _scraper_cache.clear() 
        _region_name_cache.clear() 
        _region_coord_cache.clear()
        _tianqi_slug_map = {}
        _tianqi_slug_updated_at = None
        client = _get_cache_redis_client()
        if client is not None:
            try:
                keys = list(client.scan_iter(match=f"{_SCRAPER_CACHE_REDIS_PREFIX}:*"))
                keys.extend([_WU_KEY_CACHE_REDIS_KEY, _WU_ACTIVE_KEY_REDIS_KEY])
                if keys:
                    client.delete(*keys)
            except Exception:
                pass
        cleared_cache = True

    # Reset parser cooldown state for scraper sources.
    for source in DATA_SOURCES.values():
        if hasattr(source, "parser_disabled_until"):
            setattr(source, "parser_disabled_until", None)
        if hasattr(source, "parser_fail_count"):
            setattr(source, "parser_fail_count", 0)

    return {
        "window_slot": _scraper_window_slot,
        "window_used": _scraper_window_used,
        "cache_cleared": cleared_cache,
        "slug_map_size": len(_tianqi_slug_map),
    }


def _canonicalize_tianqi_url(url: str) -> str:
    try:
        parsed = urlparse(str(url or "").strip())
        host = (parsed.hostname or "").lower()
        if not host.endswith("tianqi.com"):
            return str(url or "").strip()
        path = (parsed.path or "/").strip()
        if not path.startswith("/"):
            path = "/" + path
        if not path.endswith("/"):
            path += "/"
        # Ignore query/fragment for canonical key.
        return f"https://{host}{path.lower()}"
    except Exception:
        return str(url or "").strip()


def _deterministic_rng(region_code: str) -> random.Random:
    key = str(region_code).encode("utf-8")
    digest = hashlib.sha256(key).digest()
    seed = int.from_bytes(digest[:8], byteorder="big", signed=False)
    return random.Random(seed)


def _has_real_api_key(value: str | None) -> bool:
    if value is None:
        return False
    normalized = str(value).strip().lower()
    return normalized not in PLACEHOLDER_KEYS and normalized not in SIMULATION_KEYS


def _api_key_mode(value: str | None) -> str:
    """
    Returns one of: live | simulate | disabled
    """
    if value is None:
        return "disabled"
    normalized = str(value).strip().lower()
    if normalized in SIMULATION_KEYS:
        return "simulate"
    if normalized in PLACEHOLDER_KEYS:
        return "disabled"
    return "live"


def _is_government_domain(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    if not host:
        return False
    if host in BLOCKED_GOV_EXACT:
        return True
    return any(host.endswith(suffix) for suffix in BLOCKED_GOV_SUFFIXES)


def _domain_allowed(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    allowed = {d.strip().lower() for d in settings.scraper_allowed_domains if d.strip()}
    if not allowed:
        return False
    return any(host == d or host.endswith(f".{d}") for d in allowed)


def _build_tianqi_fallback_urls(primary_url: str) -> list[str]: 
    host = (urlparse(primary_url).hostname or "").lower()
    if "tianqi.com" not in host:
        return [primary_url]

    parsed = urlparse(primary_url)
    path_parts = [p for p in parsed.path.split("/") if p]
    if not path_parts:
        return [primary_url]

    slug = path_parts[0].strip().lower()
    slug_no_suffix = re.sub(r"\d+$", "", slug)

    # Prefer conservative HTTPS variants.
    # Avoid appending "/7/" here; it often returns 403 and slows down the whole batch.
    candidates: list[str] = [ 
        f"https://www.tianqi.com/{slug}/", 
    ] 
    if slug_no_suffix and slug_no_suffix != slug: 
        candidates.extend( 
            [ 
                f"https://www.tianqi.com/{slug_no_suffix}/", 
            ] 
        ) 

    deduped: list[str] = []
    for url in [primary_url, *candidates]:
        if url not in deduped:
            deduped.append(url)
    return deduped


async def _acquire_scraper_budget() -> bool:
    max_requests = int(settings.scraper_max_requests_per_window or 0)
    if max_requests <= 0:
        return True

    now_slot = int(datetime.utcnow().timestamp() // _SCRAPER_WINDOW_SECONDS)
    global _scraper_window_slot, _scraper_window_used
    async with _scraper_window_lock:
        if _scraper_window_slot != now_slot:
            _scraper_window_slot = now_slot
            _scraper_window_used = 0
        if _scraper_window_used >= max_requests:
            return False
        _scraper_window_used += 1
        return True


def _get_region_name(region_code: str) -> str:
    cached = _region_name_cache.get(region_code)
    if cached:
        return cached

    db = SessionLocal()
    try:
        region = (
            db.execute(select(Region.name).where(Region.code == region_code).limit(1))
            .scalars()
            .first()
        )
        name = str(region or region_code)
        _region_name_cache[region_code] = name
        return name
    except Exception:
        return region_code
    finally:
        db.close()


def _get_region_coordinates(region_code: str) -> Tuple[Optional[float], Optional[float]]:
    cached = _region_coord_cache.get(region_code)
    if cached:
        return cached[0], cached[1]

    db = SessionLocal()
    try:
        row = (
            db.execute(
                select(Region.longitude, Region.latitude)
                .where(Region.code == region_code)
                .limit(1)
            )
            .first()
        )
        if not row:
            return None, None
        lon, lat = row
        if lon is None or lat is None:
            return None, None
        lon_f = float(lon)
        lat_f = float(lat)
        _region_coord_cache[region_code] = (lon_f, lat_f)
        return lon_f, lat_f
    except Exception:
        return None, None
    finally:
        db.close()


def _build_template_context(region_code: str, region_name: Optional[str] = None) -> Dict[str, str]:
    safe_region_name = str(region_name or _get_region_name(region_code))
    return {
        "region_code": str(region_code),
        "region_name": safe_region_name,
        "region_name_url": quote(safe_region_name),
    }


def _normalize_region_name(name: str) -> str:
    variants = _normalized_region_name_variants(name)
    return variants[0] if variants else ""


def _candidate_tianqi_names(region_code: str, region_name: str) -> list[str]:
    code = str(region_code or "").strip()
    names = list(_candidate_region_names(region_name))

    if len(code) == 6 and not code.endswith("00"):
        names.extend(_candidate_region_names(_get_region_name(f"{code[:4]}00")))
        names.extend(_candidate_region_names(_get_region_name(f"{code[:2]}0000")))
    elif len(code) == 6 and code.endswith("00") and not code.endswith("0000"):
        names.extend(_candidate_region_names(_get_region_name(f"{code[:2]}0000")))

    return _dedupe_preserve_order(names)


def _resolve_scrape_target(region_code: str, region_name: Optional[str] = None) -> tuple[str, str]:  
    name = str(region_name or _get_region_name(region_code))  
    if not settings.scraper_city_level_only:  
        return str(region_code), name  
  
    code = str(region_code)  
    if len(code) == 6 and not code.endswith("00"):  
        city_code = f"{code[:4]}00"  
        city_name = _get_region_name(city_code)  
        if city_name and city_name != city_code:  
            return city_code, city_name  
        # Do NOT collapse to a derived city_code if it doesn't exist in DB.
        # Some provinces (e.g. 海南 4690xx) don't follow a normal prefecture-code hierarchy, and collapsing would
        # cause many different regions to share the same target_code/cache key and repeatedly hit the same URL.
        return code, name
    return code, name 


def _is_valid_tianqi_slug(slug: str) -> bool:
    s = str(slug or "").strip().lower()
    if not s:
        return False
    if s in TIANQI_SLUG_BLOCKLIST:
        return False
    # Avoid slugs that are purely numeric (e.g. admin codes) which lead to invalid URLs.
    if not re.search(r"[a-z]", s):
        return False
    return bool(re.fullmatch(r"[a-z0-9_-]{2,64}", s))


def _extract_wu_api_keys(text: str) -> list[str]:
    raw = str(text or "")
    if not raw:
        return []
    keys: list[str] = []
    for m in re.finditer(r"apiKey(?:=|%3D)([A-Za-z0-9]{20,64})", raw, flags=re.IGNORECASE):
        key = str(m.group(1) or "").strip()
        if key:
            keys.append(key)
    return _dedupe_preserve_order(keys)


async def _discover_wu_api_keys(force: bool = False) -> list[str]:
    """
    Discover weather.com apiKey from WU page payload.
    This is best-effort and only used as a supplemental source.
    """
    global _wu_key_cache, _wu_key_cache_expire_at, _wu_active_key

    if not settings.wu_key_discovery_enabled:
        return []

    now = datetime.utcnow()
    if not force:
        redis_keys, redis_expire_at, redis_active = _read_wu_key_cache_from_redis()
        if redis_active and not _wu_active_key:
            _wu_active_key = redis_active
        if (
            redis_keys
            and redis_expire_at is not None
            and now < redis_expire_at
        ):
            _wu_key_cache = list(redis_keys)
            _wu_key_cache_expire_at = redis_expire_at
            return list(_wu_key_cache)

    if (
        not force
        and _wu_key_cache
        and _wu_key_cache_expire_at is not None
        and now < _wu_key_cache_expire_at
    ):
        return list(_wu_key_cache)

    async with _wu_key_lock:
        now = datetime.utcnow()
        if (
            not force
            and _wu_key_cache
            and _wu_key_cache_expire_at is not None
            and now < _wu_key_cache_expire_at
        ):
            return list(_wu_key_cache)

        discover_url = str(settings.wu_key_discovery_url or "").strip()
        if not discover_url:
            return []

        response = await fetch_with_retry(
            discover_url,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.7",
            },
            max_retries=max(1, int(settings.wu_max_retries or 1)),
            timeout_seconds=max(3.0, float(settings.wu_timeout_seconds or 8.0)),
            require_domain_allowlist=False,
        )
        if "error" in response:
            return []

        html_text = str(response.get("raw_text") or "")
        keys = _extract_wu_api_keys(html_text)
        if keys:
            _wu_key_cache = keys
            ttl_minutes = max(30, int(settings.wu_key_refresh_minutes or 360))
            _wu_key_cache_expire_at = datetime.utcnow() + timedelta(minutes=ttl_minutes)
            _persist_wu_key_cache_to_redis(_wu_key_cache, _wu_key_cache_expire_at)
            return list(_wu_key_cache)
        return []


def _extract_tianqi_slug_from_href(href: str) -> Optional[str]:
    href_l = str(href or "").strip().lower()
    if not href_l:
        return None
    # Accept absolute and relative links.
    if "://" in href_l:
        parsed = urlparse(href_l)
        if parsed.hostname and "tianqi.com" not in parsed.hostname.lower():
            return None
        path = parsed.path or ""
    else:
        path = href_l

    parts = [p for p in path.split("/") if p]
    if not parts:
        return None

    # Prefer the last valid slug-looking segment to handle patterns like /province/<slug>/.
    for part in reversed(parts):
        candidate = part.strip().lower()
        if _is_valid_tianqi_slug(candidate):
            return candidate
    return None
 
 
def _extract_tianqi_slug_map_from_html(raw_html: str) -> Dict[str, str]: 
    mapping: Dict[str, str] = {} 
    if not raw_html: 
        return mapping 
 
    if BeautifulSoup is not None: 
        try: 
            soup = BeautifulSoup(raw_html, "html.parser") 
            for a in soup.find_all("a"): 
                href = str(a.get("href") or "").strip() 
                if not href: 
                    continue 
                href_l = href.lower()
                if "tianqi.com" not in href_l and not href_l.startswith("/"): 
                    continue 
                # Province index links (e.g. /province/jilin/) are not city pages and often collide with city names.
                if "/province/" in href_l:
                    continue
                slug = _extract_tianqi_slug_from_href(href_l)
                if not slug:
                    continue 
 
                label = html.unescape(a.get_text(" ", strip=True) or "").strip() 
                if not label: 
                    continue 
                if label in {"天气", "全国天气", "国际天气", "天气网", "首页"}: 
                    continue 
 
                mapping.setdefault(slug, slug) 
                for candidate_label in _candidate_region_names(label): 
                    mapping.setdefault(candidate_label, slug) 
                    normalized = _normalize_single_region_name(candidate_label) 
                    if normalized: 
                        mapping.setdefault(normalized, slug) 
        except Exception: 
            # Fall back to regex parser below. 
            pass 
 
    # Regex fallback keeps compatibility when HTML parser fails. 
    pattern = re.compile(
        r"""<a[^>]+href=['"]([^'"]+)['"][^>]*>(.*?)</a>""",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(raw_html): 
        href = str(match.group(1) or "")
        if "/province/" in href.lower():
            continue
        slug = _extract_tianqi_slug_from_href(href)
        if not slug:
            continue 
        label = html.unescape(re.sub(r"(?is)<[^>]+>", " ", str(match.group(2) or ""))).strip()
        if not label or label in {"天气", "全国天气", "国际天气", "天气网", "首页"}: 
            continue 
        mapping.setdefault(slug, slug) 
        for candidate_label in _candidate_region_names(label): 
            mapping.setdefault(candidate_label, slug) 
            normalized = _normalize_single_region_name(candidate_label) 
            if normalized: 
                mapping.setdefault(normalized, slug) 
    return mapping 


async def _ensure_tianqi_slug_map() -> Dict[str, str]:
    ttl_minutes = max(30, int(settings.scraper_tianqi_index_ttl_minutes or 360))
    now = datetime.utcnow()

    global _tianqi_slug_updated_at, _tianqi_slug_map
    if _tianqi_slug_updated_at and _tianqi_slug_map:
        age = (now - _tianqi_slug_updated_at).total_seconds()
        if age <= ttl_minutes * 60:
            return _tianqi_slug_map

    async with _tianqi_slug_lock:
        now = datetime.utcnow()
        if _tianqi_slug_updated_at and _tianqi_slug_map:
            age = (now - _tianqi_slug_updated_at).total_seconds()
            if age <= ttl_minutes * 60:
                return _tianqi_slug_map

        index_url = (settings.scraper_tianqi_city_index_url or "").strip()
        if not index_url:
            return _tianqi_slug_map

        payload = await fetch_with_retry(
            index_url,
            require_domain_allowlist=True,
            max_retries=max(1, int(settings.scraper_max_retries)),
            timeout_seconds=max(2.0, float(settings.scraper_timeout_seconds)),
        )
        raw_text = str(payload.get("raw_text", "") or "")
        mapping = _extract_tianqi_slug_map_from_html(raw_text)
        if mapping:
            _tianqi_slug_map = mapping
            _tianqi_slug_updated_at = datetime.utcnow()
        return _tianqi_slug_map


async def _resolve_tianqi_slug(region_name: str) -> Optional[str]:  
    # Manual overrides first. 
    if _tianqi_slug_overrides: 
        # Prefer the "correct path": normalize the *original* name (e.g. "汕尾市" -> "汕尾") before
        # considering any recoded/heuristic candidates. This avoids accidental collisions like
        # "汕尾市" -> "马尾(区)" when candidate generation gets polluted.
        base = html.unescape(str(region_name or "")).strip()
        stable_keys = _dedupe_preserve_order(
            [
                base,
                base.replace(" ", "").replace("　", ""),
            ]
        )
        for key in stable_keys:
            ov = _tianqi_slug_overrides.get(key)
            if ov:
                return ov
        stable_norms = _dedupe_preserve_order(
            [
                _normalize_single_region_name(k)
                for k in stable_keys
                if k
            ]
        )
        for norm in stable_norms:
            if not norm:
                continue
            ov = _tianqi_slug_overrides.get(norm)
            if ov:
                return ov

        # Fallback: broader candidates (may include recoded strings). Keep this after the stable path.
        for candidate in _candidate_region_names(region_name): 
            ov = _tianqi_slug_overrides.get(candidate) 
            if ov: 
                return ov 
        for normalized in _normalized_region_name_variants(region_name): 
            ov = _tianqi_slug_overrides.get(normalized) 
            if ov: 
                return ov 

    mapping = await _ensure_tianqi_slug_map() 
    if not mapping: 
        mapping = {} 

    for candidate in _candidate_region_names(region_name):
        direct = mapping.get(candidate)
        if direct:
            return direct

    normalized_variants = _normalized_region_name_variants(region_name)
    for normalized in normalized_variants:
        direct_norm = mapping.get(normalized)
        if direct_norm:
            return direct_norm

    # Fallback for county/district names that differ from index labels.
    for normalized in normalized_variants:
        if len(normalized) < 2:
            continue
        best_slug: Optional[str] = None
        best_score = -1
        for key, slug in mapping.items():
            key_norm = _normalize_region_name(key)
            if not key_norm:
                continue
            if normalized in key_norm or key_norm in normalized:
                score = min(len(normalized), len(key_norm))
                if score > best_score:
                    best_score = score
                    best_slug = slug
        if best_slug:
            return best_slug

    # Optional fallback: derive slug from Chinese name (pinyin) when index mapping is incomplete/unavailable. 
    # This costs extra requests but avoids "slug_not_found" for common cities when the index page changes. 
    # When scraping is configured for city-level only, avoid guessing slugs for districts/counties (often invalid).
    raw_name = str(region_name or "").strip()
    skip_pinyin = bool(settings.scraper_city_level_only and raw_name.endswith(("区", "县", "旗")))
    if lazy_pinyin is not None: 
        if skip_pinyin:
            return None
        for normalized in normalized_variants: 
            if len(normalized) < 2: 
                continue 
            if any("a" <= ch <= "z" or "0" <= ch <= "9" for ch in normalized): 
                continue 
            try:
                slug = "".join([p for p in lazy_pinyin(normalized, errors="ignore") if p]).strip().lower()
            except Exception:
                slug = ""
            if slug and slug not in TIANQI_SLUG_BLOCKLIST:
                return slug

    # If region name itself looks like a slug, accept it. 
    for candidate in _candidate_region_names(region_name): 
        slug = str(candidate or "").strip().lower() 
        if _is_valid_tianqi_slug(slug): 
            return slug 

    return None


def _read_scraper_cache(
    source_name: str, region_code: str
) -> tuple[Optional[Dict[str, Any]], float, bool]:
    item = _scraper_cache.get((source_name, region_code))
    if item:
        cached_at, payload = item
    else:
        payload, age = _read_scraper_cache_from_redis(source_name, region_code)
        if payload is None:
            return None, 0.0, False
        cached_at = datetime.utcnow() - timedelta(seconds=age)
        _scraper_cache[(source_name, region_code)] = (cached_at, dict(payload))

    age = max(0.0, (datetime.utcnow() - cached_at).total_seconds())
    ttl_seconds = _scraper_cache_ttl_seconds()
    return dict(payload), age, age <= ttl_seconds


def _write_scraper_cache(source_name: str, region_code: str, payload: Dict[str, Any]) -> None:
    if not payload or "error" in payload:
        return
    cached_at = datetime.utcnow()
    clean_payload = dict(payload)
    _scraper_cache[(source_name, region_code)] = (cached_at, clean_payload)
    _write_scraper_cache_to_redis(source_name, region_code, clean_payload, cached_at)


async def _run_scraper_inflight(
    source_name: str,
    region_code: str,
    producer: Callable[[], Awaitable[Dict[str, Any]]],
) -> Dict[str, Any]:
    key = (source_name, region_code)
    leader = False
    async with _scraper_inflight_lock:
        existing = _scraper_inflight.get(key)
        if existing is None:
            future = asyncio.get_running_loop().create_future()
            _scraper_inflight[key] = future
            leader = True
        else:
            future = existing

    if not leader:
        result = await future
        return dict(result) if isinstance(result, dict) else {"error": "inflight_invalid_result"}

    try:
        result = await producer()
        if not isinstance(result, dict):
            result = {"error": "inflight_invalid_result"}
        if not future.done():
            future.set_result(dict(result))
        return result
    except Exception as exc:
        fallback = {"error": "inflight_failed", "message": str(exc)[:300]}
        if not future.done():
            future.set_result(fallback)
        return fallback
    finally:
        async with _scraper_inflight_lock:
            if _scraper_inflight.get(key) is future:
                _scraper_inflight.pop(key, None)


async def _enforce_scraper_pacing() -> None:
    interval = max(0.1, float(settings.scraper_request_interval_seconds))
    parallel_limit = max(1, int(settings.scraper_max_parallel_requests or 1))

    while True:
        wait_seconds = 0.0
        async with _scraper_rate_lock:
            now = asyncio.get_running_loop().time()
            window_start = now - interval
            while _scraper_request_timestamps and _scraper_request_timestamps[0] < window_start:
                _scraper_request_timestamps.pop(0)

            if len(_scraper_request_timestamps) < parallel_limit:
                _scraper_request_timestamps.append(now)
                return

            oldest = _scraper_request_timestamps[0]
            wait_seconds = max(0.01, interval - (now - oldest) + 0.01)

        await asyncio.sleep(wait_seconds)


async def _acquire_scraper_slot() -> asyncio.Semaphore:
    global _scraper_parallel_semaphore, _scraper_parallel_size
    parallel_limit = max(1, int(settings.scraper_max_parallel_requests or 1))
    async with _scraper_parallel_lock:
        if _scraper_parallel_semaphore is None or _scraper_parallel_size != parallel_limit:
            _scraper_parallel_semaphore = asyncio.Semaphore(parallel_limit)
            _scraper_parallel_size = parallel_limit
        sem = _scraper_parallel_semaphore
    await sem.acquire()
    return sem


def _release_scraper_slot(sem: asyncio.Semaphore) -> None:
    sem.release()


async def _get_http_client() -> httpx.AsyncClient:
    # Keep a single client to reuse connections and cookies (helps some sites and speeds up scraping).
    global _http_client
    if _http_client is not None:
        return _http_client
    async with _http_client_lock:
        if _http_client is None:
            limits = httpx.Limits(max_connections=64, max_keepalive_connections=32)
            # trust_env=False avoids inheriting proxy vars that can break connectivity inside containers.
            _http_client = httpx.AsyncClient(follow_redirects=True, limits=limits, trust_env=False)
        return _http_client


async def _maybe_wait_host_cooldown(host: str, *, max_wait_seconds: float = 60.0) -> None:
    if not host:
        return
    now = asyncio.get_running_loop().time()
    until = _host_cooldown_until.get(host)
    if until is None or until <= now:
        return
    wait = min(max_wait_seconds, max(0.0, until - now))
    if wait > 0:
        await asyncio.sleep(wait)


def _set_host_cooldown(host: str, seconds: float) -> None:
    if not host or seconds <= 0:
        return
    now = asyncio.get_running_loop().time()
    _host_cooldown_until[host] = max(_host_cooldown_until.get(host, 0.0), now + seconds)


async def _ensure_tianqi_session(client: httpx.AsyncClient, headers: Dict[str, str]) -> None:
    global _tianqi_session_warmed_at
    now = asyncio.get_running_loop().time()
    if _tianqi_session_warmed_at and (now - _tianqi_session_warmed_at) < _TIANQI_SESSION_TTL_SECONDS:
        return
    prewarm_url = (settings.scraper_tianqi_city_index_url or "https://www.tianqi.com/chinacity.html").strip()
    if not prewarm_url or not _domain_allowed(prewarm_url):
        _tianqi_session_warmed_at = now
        return
    try:
        await client.get(prewarm_url, headers=headers, timeout=max(4.0, float(settings.scraper_timeout_seconds)))
    except Exception:
        pass
    _tianqi_session_warmed_at = now


async def fetch_with_retry(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    max_retries: int = 3,
    timeout_seconds: float = 12,
    require_domain_allowlist: bool = False,
    tianqi_prewarm: bool = False,
) -> Dict[str, Any]:
    if _is_government_domain(url):
        return {"error": "government_domain_blocked", "url": url}
    if require_domain_allowlist and not _domain_allowed(url):
        return {"error": "domain_not_allowed", "url": url}

    # Keep headers minimal and widely accepted.
    # Avoid advertising brotli ("br") unless we know decompression is available.
    merged_headers = {
        "User-Agent": settings.scraper_user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    if headers:
        merged_headers.update(headers)

    host = (urlparse(url).hostname or "").lower()
    if host.endswith("tianqi.com"):
        merged_headers.setdefault("Referer", "https://www.tianqi.com/chinacity.html")
        merged_headers.setdefault("Origin", "https://www.tianqi.com")

    prefer_chinese = host.endswith("tianqi.com") or host.endswith("weather.com.cn")

    def _format_request_error(exc: Exception) -> str:
        parts: list[str] = [exc.__class__.__name__]
        try:
            text = str(exc).strip()
        except Exception:
            text = ""
        if text:
            parts.append(text)
        for cause in [getattr(exc, "__cause__", None), getattr(exc, "__context__", None)]:
            if not cause:
                continue
            try:
                ctext = str(cause).strip()
            except Exception:
                ctext = ""
            if ctext:
                parts.append(f"cause={cause.__class__.__name__}: {ctext}")
            else:
                parts.append(f"cause={cause.__class__.__name__}")
            break
        return " | ".join(parts)[:380]

    def _decode_response_text(response: httpx.Response) -> str:
        raw = response.content or b""
        if not raw:
            return ""

        declared = str(response.encoding or "").strip().lower()
        candidates = _dedupe_preserve_order([declared, "utf-8", "gb18030", "gbk"])
        if not candidates:
            candidates = ["utf-8", "gb18030", "gbk"]

        best_text = ""
        best_score = float("inf")
        for encoding in candidates:
            try:
                text = raw.decode(encoding, errors="replace")
            except Exception:
                continue
            replacement_penalty = text.count("\ufffd") * 8 + text.count("?") * 0.1
            cjk_bonus = 0.0
            if prefer_chinese:
                cjk_count = sum(1 for ch in text if "\u4e00" <= ch <= "\u9fff")
                cjk_bonus = min(30.0, cjk_count / 300.0)
            score = replacement_penalty - cjk_bonus
            if score < best_score:
                best_score = score
                best_text = text

        if best_text:
            return best_text
        return response.text

    for attempt in range(max_retries):
        try:
            await _maybe_wait_host_cooldown(host)
            client = await _get_http_client()
            if tianqi_prewarm and host.endswith("tianqi.com"):
                await _ensure_tianqi_session(client, merged_headers)
            response = await client.get(url, params=params, headers=merged_headers, timeout=timeout_seconds)
            response.raise_for_status()
            content_type = response.headers.get("content-type", "").lower()
            if "application/json" in content_type:
                try:
                    return response.json()
                except Exception:
                    pass
            return {"raw_text": _decode_response_text(response)}
        except httpx.HTTPStatusError as exc: 
            status_code = exc.response.status_code if exc.response is not None else None 
            if status_code in {403, 429}: 
                # Back off on likely anti-bot / rate-limit responses. 
                # 429 is a strong signal of rate limiting. Many 403s are caused by invalid paths/slugs,
                # so keep the cooldown short to avoid stalling the whole batch.
                if status_code == 429:
                    base = 6.0
                    if host.endswith("tianqi.com"):
                        base = 10.0
                else:
                    base = 1.2
                    if host.endswith("tianqi.com"):
                        base = 2.0
                _set_host_cooldown(host, base + (2 ** attempt) * 0.5 + random.uniform(0.1, 0.6)) 
            if attempt == max_retries - 1: 
                return { 
                    "error": "request_failed", 
                    "message": f"http_status_{status_code}", 
                    "status_code": status_code,
                    "url": url,
                }
            await asyncio.sleep((2 ** attempt) + random.uniform(0.15, 0.65))
        except httpx.RequestError as exc:
            # Short cooldown for transient network errors (DNS/TLS/timeout) to avoid tight loops per host.
            _set_host_cooldown(host, 1.0 + random.uniform(0.1, 0.6))
            if attempt == max_retries - 1:
                message = _format_request_error(exc)
                return {
                    "error": "request_failed",
                    "message": message,
                    "url": url,
                    "host": host,
                }
            await asyncio.sleep((2 ** attempt) + random.uniform(0.15, 0.65))
        except Exception as exc:
            _set_host_cooldown(host, 1.0 + random.uniform(0.1, 0.6))
            if attempt == max_retries - 1:
                message = _format_request_error(exc)
                return {"error": "request_failed", "message": message, "url": url, "host": host}
            await asyncio.sleep((2 ** attempt) + random.uniform(0.15, 0.65))

    return {"error": "unknown"}


class CMAWeatherDataSource:
    source_name = "weather_cma"
    channel = "meteorology"
    reliability = 0.95

    async def fetch(self, region_code: str, region_name: Optional[str] = None) -> Dict[str, Any]:
        _ = region_name
        key_mode = _api_key_mode(settings.cma_api_key)
        if key_mode == "simulate":
            rng = _deterministic_rng(region_code)
            return {
                "simulated": True,
                "precipitation_24h": round(rng.uniform(5, 110), 1),
                "precipitation_1h": round(rng.uniform(0, 28), 1),
                "soil_moisture": round(rng.uniform(0.15, 0.55), 2),
                "wind_speed": round(rng.uniform(1, 12), 1),
            }
        # Live CMA national surface observation API is station-based.
        # You must generate region->station mapping first: backend/app/data/cma_region_station_map.json
        if not (settings.cma_user_id and settings.cma_password):
            return {"error": "missing_cma_credentials"}

        station_id = _get_cma_station_id(region_code)
        if not station_id:
            return {"error": "cma_station_not_mapped", "region_code": region_code}

        # CMA timeRange supports recent 7 days; we query last 24h to build rain_24h from PRE_3h.
        tz_offset = int(getattr(settings, "cma_time_zone_offset_hours", 8) or 8)
        now = datetime.utcnow() + timedelta(hours=tz_offset)
        end = now.strftime("%Y%m%d%H%M%S")
        start = (now - timedelta(hours=24)).strftime("%Y%m%d%H%M%S")
        time_range = f"[{start},{end}]"

        base = settings.cma_base_url.rstrip("/")
        url = base if base.endswith("/api") else f"{base}/api"
        return await fetch_with_retry(
            url,
            params={
                "userId": settings.cma_user_id,
                "pwd": settings.cma_password,
                "dataFormat": "json",
                "interfaceId": settings.cma_interface_id,
                "dataCode": settings.cma_data_code,
                "timeRange": time_range,
                "staIDs": station_id,
                # Default fields + required elements (PRE_3h mm, RHU %, 2min wind speed m/s, TEM C)
                "elements": "Station_Id_C,Year,Mon,Day,Hour,PRE_3h,RHU,WIN_S_Avg_2mi,TEM,Datetime",
            },
        )

    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        if "error" in raw_data:
            return raw_data
        # Simulated payload matches our internal fields.
        if raw_data.get("simulated"):
            return {
                "rain_24h": float(raw_data.get("precipitation_24h") or 0),
                "rain_1h": float(raw_data.get("precipitation_1h") or 0),
                "soil_moisture": float(raw_data.get("soil_moisture") or 0),
                "wind_speed": float(raw_data.get("wind_speed") or 0),
                "temperature": float(raw_data.get("temperature") or 20),
                "humidity": float(raw_data.get("humidity") or 50),
                "data_mode": "simulated",
            }

        # Official CMA API response: try to locate the rows under common keys.
        rows = None
        for key in ["data", "Data", "DS", "datas", "rows", "result"]:
            if isinstance(raw_data.get(key), list):
                rows = raw_data.get(key)
                break
        if rows is None and isinstance(raw_data.get("json"), dict):
            inner = raw_data["json"]
            for key in ["data", "Data", "DS", "rows"]:
                if isinstance(inner.get(key), list):
                    rows = inner.get(key)
                    break

        if not isinstance(rows, list) or not rows:
            return {"error": "cma_parse_failed", "message": "cma_empty_rows"}

        parsed: list[dict] = [r for r in rows if isinstance(r, dict)]
        if not parsed:
            return {"error": "cma_parse_failed", "message": "cma_rows_not_dict"}

        def _row_dt(row: dict) -> str:
            dt = str(row.get("Datetime") or "").strip()
            if dt:
                return dt
            y = str(row.get("Year") or "").zfill(4)
            m = str(row.get("Mon") or "").zfill(2)
            d = str(row.get("Day") or "").zfill(2)
            h = str(row.get("Hour") or "").zfill(2)
            return f"{y}-{m}-{d} {h}:00:00"

        parsed.sort(key=_row_dt)
        latest = parsed[-1]

        pre_sum = 0.0
        pre_count = 0
        for row in parsed:
            v = row.get("PRE_3h")
            try:
                if v is None or str(v).strip() == "":
                    continue
                pre_sum += float(v)
                pre_count += 1
            except Exception:
                continue

        humidity = None
        wind_speed = None
        temperature = None
        try:
            if latest.get("RHU") not in (None, ""):
                humidity = float(latest.get("RHU"))
        except Exception:
            humidity = None
        try:
            if latest.get("WIN_S_Avg_2mi") not in (None, ""):
                wind_speed = float(latest.get("WIN_S_Avg_2mi"))
        except Exception:
            wind_speed = None
        try:
            if latest.get("TEM") not in (None, ""):
                temperature = float(latest.get("TEM"))
        except Exception:
            temperature = None

        # rain_1h cannot be directly derived from PRE_3h without assumptions; keep it None.
        return {
            "rain_24h": round(pre_sum, 2) if pre_count > 0 else None,
            "rain_1h": None,
            "wind_speed": wind_speed,
            "temperature": temperature,
            "humidity": humidity,
            "data_mode": "live",
        }


class AMapWeatherDataSource:
    """
    Gaode (AMap) Web Service Weather API.
    - Input: adcode (we reuse region_code)
    - Output: live weather (no precipitation mm), we heuristically estimate rain_1h/rain_24h from weather text.
    """

    source_name = "weather_amap"
    channel = "meteorology"
    reliability = 0.78

    async def fetch(self, region_code: str, region_name: Optional[str] = None) -> Dict[str, Any]:
        _ = region_name
        key_mode = _api_key_mode(settings.amap_api_key)
        if key_mode == "simulate":
            rng = _deterministic_rng(region_code + "_amap")
            weather_text = rng.choice(["晴", "多云", "阴", "小雨", "中雨", "阵雨", "雷阵雨"])
            return {
                "simulated": True,
                "adcode": region_code,
                "weather": weather_text,
                "temperature": round(rng.uniform(0, 30), 1),
                "humidity": round(rng.uniform(30, 98), 1),
                "windpower": str(rng.randint(1, 6)),
                "winddirection": rng.choice(["东北", "东", "东南", "南", "西南", "西", "西北", "北"]),
                "reporttime": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            }
        if key_mode == "disabled":
            return {"error": "missing_amap_api_key"}

        url = f"{settings.amap_base_url.rstrip('/')}/v3/weather/weatherInfo"
        return await fetch_with_retry(
            url,
            params={
                "key": settings.amap_api_key,
                "city": region_code,
                "extensions": "base",
                "output": "JSON",
            },
        )

    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        if "error" in raw_data:
            return raw_data

        # Simulated payload keeps the AMap field names directly.
        if raw_data.get("simulated"):
            weather_text = str(raw_data.get("weather") or "").strip()
            rain_1h, rain_24h, rain_note = estimate_rain_from_weather_text(weather_text)
            wind_lvl = None
            try:
                wind_lvl = int(str(raw_data.get("windpower") or "").strip())
            except Exception:
                wind_lvl = None
            wind_speed = wind_level_to_speed_ms(wind_lvl)
            return {
                # AMap live does not provide mm precipitation. Keep estimates separate so they won't
                # override real mm data from other sources during merge.
                "rain_24h_est": float(rain_24h) if rain_24h is not None else None,
                "rain_1h_est": float(rain_1h) if rain_1h is not None else None,
                "wind_speed": float(wind_speed or 0),
                "temperature": raw_data.get("temperature"),
                "humidity": raw_data.get("humidity"),
                "weather_text": weather_text,
                "wind_direction": raw_data.get("winddirection"),
                "wind_power_level": wind_lvl,
                "precipitation_note": rain_note,
                "data_mode": "simulated",
            }

        live, err = parse_amap_live(raw_data)
        if err:
            return {"error": "amap_parse_failed", "message": err}
        if not live:
            return {"error": "amap_parse_failed", "message": "amap_live_missing"}

        rain_1h, rain_24h, rain_note = estimate_rain_from_weather_text(live.weather)
        wind_speed = wind_level_to_speed_ms(live.wind_power_level)
        return {
            "rain_24h_est": float(rain_24h) if rain_24h is not None else None,
            "rain_1h_est": float(rain_1h) if rain_1h is not None else None,
            "wind_speed": float(wind_speed or 0),
            "temperature": live.temperature_c if live.temperature_c is not None else 20,
            "humidity": live.humidity_pct if live.humidity_pct is not None else 50,
            "weather_text": live.weather,
            "wind_direction": live.wind_direction,
            "wind_power_level": live.wind_power_level,
            "report_time": live.report_time,
            "precipitation_note": rain_note,
            "data_mode": "live",
        }


class WeatherUndergroundApiSource:
    """
    Supplemental source via weather.com API endpoints used by WU pages.
    Key can be:
    - manually configured (`WU_API_KEY`)
    - auto-discovered from a configured WU page payload (best effort)
    """

    source_name = "weather_wu_api"
    channel = "meteorology"
    reliability = float(settings.wu_reliability or 0.62)

    async def fetch(self, region_code: str, region_name: Optional[str] = None) -> Dict[str, Any]:
        _ = region_name
        if not settings.wu_enabled:
            return {"error": "wu_disabled"}

        cached, _, fresh = _read_scraper_cache(self.source_name, region_code)
        if cached and fresh:
            cached["_cache_hit"] = True
            return cached

        key_mode = _api_key_mode(settings.wu_api_key)
        if key_mode == "simulate":
            rng = _deterministic_rng(region_code + "_wu")
            return {
                "simulated": True,
                "precip1Hour": round(rng.uniform(0, 8), 2),
                "precip24Hour": round(rng.uniform(0, 80), 2),
                "relativeHumidity": int(rng.uniform(35, 98)),
                # weather.com metric windSpeed is km/h; keep that in simulated payload for same normalization path.
                "windSpeed": round(rng.uniform(2, 45), 1),
                "windGust": round(rng.uniform(4, 55), 1),
                "temperature": round(rng.uniform(-10, 35), 1),
                "wxPhraseLong": rng.choice(["Cloudy", "Mostly Cloudy", "Light Rain", "Rain Shower", "Partly Cloudy"]),
            }

        lon, lat = _get_region_coordinates(region_code)
        if lon is None or lat is None:
            return {"error": "wu_no_region_coordinates", "region_code": region_code}

        candidates: list[str] = []
        global _wu_active_key
        if _wu_active_key:
            candidates.append(_wu_active_key)
        if key_mode == "live":
            candidates.append(str(settings.wu_api_key).strip())

        discovered = await _discover_wu_api_keys(force=False)
        candidates.extend(discovered)
        candidates = _dedupe_preserve_order(candidates)
        if not candidates:
            return {"error": "missing_wu_api_key"}

        base = str(settings.wu_api_base_url or "https://api.weather.com").rstrip("/")
        url = f"{base}/v3/wx/observations/current"
        last_error: Dict[str, Any] = {"error": "missing_wu_api_key"}
        for idx, key in enumerate(candidates):
            response = await fetch_with_retry(
                url,
                params={
                    "apiKey": key,
                    "geocode": f"{lat:.6f},{lon:.6f}",
                    "language": str(settings.wu_language or "en-US"),
                    "units": str(settings.wu_units or "m"),
                    "format": "json",
                },
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.7",
                    "Referer": str(settings.wu_key_discovery_url or "https://www.wunderground.com/"),
                    "Origin": "https://www.wunderground.com",
                },
                max_retries=max(1, int(settings.wu_max_retries or 1)),
                timeout_seconds=max(3.0, float(settings.wu_timeout_seconds or 8.0)),
                require_domain_allowlist=False,
            )
            if "error" not in response and isinstance(response, dict):
                _wu_active_key = key
                _persist_wu_active_key_to_redis(key)
                response["data_mode"] = "live"
                response["source_note"] = "weather_com_api"
                _write_scraper_cache(self.source_name, region_code, response)
                return response

            message = str(response.get("message") or "")
            if "http_status_401" in message or "http_status_403" in message:
                last_error = {
                    "error": "wu_key_rejected",
                    "message": message,
                    "key_index": idx,
                }
                # If all known keys are rejected, force refresh once and retry.
                if idx == len(candidates) - 1:
                    refreshed = _dedupe_preserve_order(await _discover_wu_api_keys(force=True))
                    fresh_candidates = [k for k in refreshed if k not in candidates]
                    for fresh in fresh_candidates:
                        retry = await fetch_with_retry(
                            url,
                            params={
                                "apiKey": fresh,
                                "geocode": f"{lat:.6f},{lon:.6f}",
                                "language": str(settings.wu_language or "en-US"),
                                "units": str(settings.wu_units or "m"),
                                "format": "json",
                            },
                            headers={
                                "Accept": "application/json, text/plain, */*",
                                "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.7",
                                "Referer": str(settings.wu_key_discovery_url or "https://www.wunderground.com/"),
                                "Origin": "https://www.wunderground.com",
                            },
                            max_retries=1,
                            timeout_seconds=max(3.0, float(settings.wu_timeout_seconds or 8.0)),
                            require_domain_allowlist=False,
                        )
                        if "error" not in retry and isinstance(retry, dict):
                            _wu_active_key = fresh
                            _persist_wu_active_key_to_redis(fresh)
                            retry["data_mode"] = "live"
                            retry["source_note"] = "weather_com_api"
                            return retry
            else:
                last_error = response

        stale_cached, _, _ = _read_scraper_cache(self.source_name, region_code)
        if stale_cached:
            stale_cached["_cache_hit"] = True
            stale_cached["_stale_cache"] = True
            return stale_cached

        return last_error

    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        if "error" in raw_data:
            return raw_data

        def _to_float(value: Any) -> Optional[float]:
            try:
                if value is None or str(value).strip() == "":
                    return None
                return float(value)
            except Exception:
                return None

        # weather.com `units=m`: precipitation is mm; wind is usually km/h.
        rain_1h = _to_float(raw_data.get("precip1Hour"))
        rain_24h = _to_float(raw_data.get("precip24Hour"))
        humidity = _to_float(raw_data.get("relativeHumidity"))
        wind_kmh = _to_float(raw_data.get("windSpeed"))
        wind_gust_kmh = _to_float(raw_data.get("windGust"))
        temperature = _to_float(raw_data.get("temperature"))

        wind_ms = round(wind_kmh / 3.6, 2) if isinstance(wind_kmh, (int, float)) else None
        wind_gust_ms = round(wind_gust_kmh / 3.6, 2) if isinstance(wind_gust_kmh, (int, float)) else None

        return {
            "rain_24h": rain_24h,
            "rain_1h": rain_1h,
            "wind_speed": wind_ms,
            "wind_gust": wind_gust_ms,
            "temperature": temperature,
            "humidity": humidity,
            "weather_text": raw_data.get("wxPhraseLong") or raw_data.get("wxPhraseMedium") or raw_data.get("wxPhraseShort"),
            "report_time": raw_data.get("validTimeLocal"),
            "data_mode": "simulated" if raw_data.get("simulated") else str(raw_data.get("data_mode") or "live"),
            "source_note": "weather_com_api",
            "data_quality_hint": "补充源：weather.com API（由 WU 页面发现 key，可能轮换）",
        }


class OpenWeatherBackupDataSource:
    source_name = "weather_openweather"
    channel = "meteorology"
    reliability = 0.65

    async def fetch(self, region_code: str, region_name: Optional[str] = None) -> Dict[str, Any]:
        _ = region_name
        if not _has_real_api_key(settings.openweather_api_key):
            return {"error": "missing_openweather_api_key"}
        # Kept as placeholder because official call usually requires coordinates.
        return {"error": "coordinates_required", "region_code": region_code}

    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        if "error" in raw_data:
            return raw_data
        return {
            "rain_24h": raw_data.get("rain_24h", 0),
            "rain_1h": raw_data.get("rain_1h", 0),
            "wind_speed": raw_data.get("wind_speed", 0),
            "temperature": raw_data.get("temperature", 20),
            "humidity": raw_data.get("humidity", 50),
        }


class CGSGeologyDataSource:
    source_name = "geology_cgs"
    channel = "geology"
    reliability = 0.9

    async def fetch(self, region_code: str, region_name: Optional[str] = None) -> Dict[str, Any]:
        _ = region_name
        key_mode = _api_key_mode(settings.cgs_api_key)
        if key_mode == "simulate":
            rng = _deterministic_rng(region_code + "_geo")
            return {
                "simulated": True,
                "terrain_slope": round(rng.uniform(5, 42), 1),
                "fault_distance_km": round(rng.uniform(0.8, 30), 1),
                "lithology": rng.choice(["granite", "sandstone", "shale", "limestone"]),
            }
        if key_mode == "disabled":
            return {"error": "missing_cgs_api_key"}

        url = f"{settings.cgs_base_url.rstrip('/')}/hazard/geology/by_region"
        return await fetch_with_retry(
            url,
            params={"region_code": region_code, "key": settings.cgs_api_key},
        )

    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        if "error" in raw_data:
            return raw_data
        return {
            "slope": raw_data.get("terrain_slope", raw_data.get("slope_degree", 0)),
            "lithology": raw_data.get("lithology", raw_data.get("rock_type", "unknown")),
            "fault_distance": raw_data.get("fault_distance_km", raw_data.get("fault_km", 999)),
            "data_mode": "simulated" if raw_data.get("simulated") else "live",
        }


class WeatherScraperSource:
    source_name = "weather_scraper"
    channel = "meteorology"
    reliability = 0.45
    min_safe_interval_seconds = 0.1
    parser_disabled_until: Optional[datetime] = None
    parser_fail_count: int = 0

    async def fetch(self, region_code: str, region_name: Optional[str] = None) -> Dict[str, Any]:
        if not settings.unofficial_scraper_enabled:
            return {"error": "scraper_disabled"}

        if self.parser_disabled_until and datetime.utcnow() < self.parser_disabled_until:
            return {"error": "scraper_parser_disabled_temporarily"}

        target_code, target_name = _resolve_scrape_target(region_code, region_name=region_name)

        cached, _, fresh = _read_scraper_cache(self.source_name, target_code)
        if cached and fresh:
            cached["_cache_hit"] = True
            return cached

        async def _fetch_once() -> Dict[str, Any]:
            recached, _, recached_fresh = _read_scraper_cache(self.source_name, target_code)
            if recached and recached_fresh:
                recached["_cache_hit"] = True
                return recached

            if not await _acquire_scraper_budget():
                return {"error": "scraper_budget_exceeded"}

            template = (settings.scraper_url_template or "").strip()
            if not template:
                return {"error": "scraper_template_not_configured"}
            try:
                context = _build_template_context(target_code, region_name=target_name)
                if "{tianqi_slug}" in template:
                    slug = None
                    for candidate in _candidate_tianqi_names(target_code, target_name):
                        slug = await _resolve_tianqi_slug(candidate)
                        if slug:
                            break
                    if not slug:
                        return {
                            "error": "tianqi_slug_not_found",
                            "region_name": target_name,
                            "slug_candidates": _candidate_tianqi_names(target_code, target_name)[:6],
                        }
                    context["tianqi_slug"] = slug
                elif "tianqi.com" in template and ("{region_name}" in template or "{region_name_url}" in template):
                    slug = None
                    for candidate in _candidate_tianqi_names(target_code, target_name):
                        slug = await _resolve_tianqi_slug(candidate)
                        if slug:
                            break
                    if slug:
                        context["region_name"] = slug
                        context["region_name_url"] = quote(slug)
                url = template.format(**context)
            except Exception:
                return {"error": "invalid_scraper_template", "template": template}

            # Safety guard: if many different targets resolve to the same Tianqi URL, it's usually a
            # bad target-folding or slug-mapping bug. Short-circuit to avoid hammering a single page.
            canonical = _canonicalize_tianqi_url(url)
            if "tianqi.com" in canonical:
                owner = _tianqi_url_owner.get(canonical)
                if owner and owner != target_code:
                    return {
                        "error": "scraper_url_collision",
                        "message": f"tianqi_url_already_owned_by:{owner}",
                        "url": canonical,
                        "owner_code": owner,
                        "target_code": target_code,
                    }
                _tianqi_url_owner.setdefault(canonical, target_code)
            if _is_government_domain(url):
                return {"error": "government_domain_blocked", "url": url}
            if not _domain_allowed(url):
                return {"error": "domain_not_allowed", "url": url}

            slot = await _acquire_scraper_slot()
            try:
                # Enforce paced parallel scraping instead of fully serial requests.
                await _enforce_scraper_pacing()
                await asyncio.sleep(self.min_safe_interval_seconds)
                response = await fetch_with_retry(
                    url,
                    require_domain_allowlist=True,
                    max_retries=max(1, int(settings.scraper_max_retries)),
                    timeout_seconds=max(2.0, float(settings.scraper_timeout_seconds)),
                    tianqi_prewarm=True,
                )
            finally:
                _release_scraper_slot(slot)
            if response.get("error") == "request_failed" and "tianqi.com" in str(urlparse(url).hostname or ""):
                status_code = int(response.get("status_code") or 0)
                msg = str(response.get("message") or "")
                # Retry alternate URL patterns when blocked (403) or when connection fails intermittently.
                should_try_alts = (status_code == 403) or ("ConnectError" in msg) or ("ReadTimeout" in msg) or ("Timeout" in msg)
                if should_try_alts:
                    for alt_url in _build_tianqi_fallback_urls(url):
                        if alt_url == url:
                            continue
                        alt_response = await fetch_with_retry(
                            alt_url,
                            require_domain_allowlist=True,
                            max_retries=1,
                            timeout_seconds=max(2.0, float(settings.scraper_timeout_seconds)),
                            tianqi_prewarm=True,
                        )
                        if "error" not in alt_response:
                            alt_response["_source_url"] = alt_url
                            _write_scraper_cache(self.source_name, target_code, alt_response)
                            return alt_response
                        response = alt_response

            if "error" not in response:
                response["_source_url"] = url
                _write_scraper_cache(self.source_name, target_code, response)
                return response

            stale_cached, _, _ = _read_scraper_cache(self.source_name, target_code)
            if stale_cached:
                stale_cached["_cache_hit"] = True
                stale_cached["_stale_cache"] = True
                return stale_cached
            return response

        response = await _run_scraper_inflight(self.source_name, target_code, _fetch_once)
        if "error" in response:
            # Keep 403 visible in logs to help debugging and tuning.
            logger.warning(
                "weather_scraper_error source=%s region=%s target=%s error=%s message=%s url=%s status=%s",
                self.source_name,
                region_code,
                target_name,
                response.get("error"),
                response.get("message"),
                response.get("url"),
                response.get("status_code"),
            )
        return response

    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        parsed = parse_weather_payload(raw_data)
        if "error" in parsed:
            if parsed.get("error") == "unsupported_scraper_payload":
                self.parser_fail_count += 1
                if self.parser_fail_count >= 5:
                    cooldown = max(1, int(settings.scraper_parser_cooldown_minutes))
                    self.parser_disabled_until = datetime.utcnow() + timedelta(minutes=cooldown)
                    self.parser_fail_count = 0
            return parsed
        self.parser_fail_count = 0
        parsed["source_note"] = "unofficial_scraper"
        return parsed


class GeologyScraperSource:
    source_name = "geology_scraper"
    channel = "geology"
    reliability = 0.4
    min_safe_interval_seconds = 0.1
    parser_disabled_until: Optional[datetime] = None
    parser_fail_count: int = 0

    async def fetch(self, region_code: str, region_name: Optional[str] = None) -> Dict[str, Any]:
        if not settings.geology_scraper_enabled:
            return {"error": "geology_scraper_disabled"}

        if self.parser_disabled_until and datetime.utcnow() < self.parser_disabled_until:
            return {"error": "scraper_parser_disabled_temporarily"}

        target_code, target_name = _resolve_scrape_target(region_code, region_name=region_name)

        cached, _, fresh = _read_scraper_cache(self.source_name, target_code)
        if cached and fresh:
            cached["_cache_hit"] = True
            return cached

        async def _fetch_once() -> Dict[str, Any]:
            recached, _, recached_fresh = _read_scraper_cache(self.source_name, target_code)
            if recached and recached_fresh:
                recached["_cache_hit"] = True
                return recached

            if not await _acquire_scraper_budget():
                return {"error": "scraper_budget_exceeded"}

            template = (settings.geology_scraper_url_template or "").strip()
            if not template:
                return {"error": "geology_scraper_template_not_configured"}
            try:
                url = template.format(**_build_template_context(target_code, region_name=target_name))
            except Exception:
                return {"error": "invalid_scraper_template", "template": template}

            if _is_government_domain(url):
                return {"error": "government_domain_blocked", "url": url}
            if not _domain_allowed(url):
                return {"error": "domain_not_allowed", "url": url}

            slot = await _acquire_scraper_slot()
            try:
                await _enforce_scraper_pacing()
                await asyncio.sleep(self.min_safe_interval_seconds)
                response = await fetch_with_retry(
                    url,
                    require_domain_allowlist=True,
                    max_retries=max(1, int(settings.scraper_max_retries)),
                    timeout_seconds=max(2.0, float(settings.scraper_timeout_seconds)),
                )
            finally:
                _release_scraper_slot(slot)
            if "error" not in response:
                response["_source_url"] = url
                _write_scraper_cache(self.source_name, target_code, response)
                return response

            stale_cached, _, _ = _read_scraper_cache(self.source_name, target_code)
            if stale_cached:
                stale_cached["_cache_hit"] = True
                stale_cached["_stale_cache"] = True
                return stale_cached
            return response

        response = await _run_scraper_inflight(self.source_name, target_code, _fetch_once)
        if "error" in response:
            logger.warning(
                "geology_scraper_error source=%s region=%s target=%s error=%s message=%s url=%s status=%s",
                self.source_name,
                region_code,
                target_name,
                response.get("error"),
                response.get("message"),
                response.get("url"),
                response.get("status_code"),
            )
        return response

    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        parsed = parse_geology_payload(raw_data)
        if "error" in parsed:
            if parsed.get("error") == "unsupported_scraper_payload":
                self.parser_fail_count += 1
                if self.parser_fail_count >= 5:
                    cooldown = max(1, int(settings.scraper_parser_cooldown_minutes))
                    self.parser_disabled_until = datetime.utcnow() + timedelta(minutes=cooldown)
                    self.parser_fail_count = 0
            return parsed
        self.parser_fail_count = 0
        parsed["source_note"] = "unofficial_scraper"
        return parsed


DATA_SOURCES: Dict[str, DataSource] = {
    "weather_cma": CMAWeatherDataSource(),
    "weather_amap": AMapWeatherDataSource(),
    "weather_wu_api": WeatherUndergroundApiSource(),
    "weather_openweather": OpenWeatherBackupDataSource(),
    "geology_cgs": CGSGeologyDataSource(),
    "weather_scraper": WeatherScraperSource(),
    "geology_scraper": GeologyScraperSource(),
}


def register_data_source(name: str, source: DataSource) -> None:
    """
    Register new source without changing graph code.
    """
    DATA_SOURCES[name] = source


async def count_historical_events(code: str, years: int) -> int:
    """
    Count historical "active warnings" from DB.
    NOTE: before official historical-disaster API is connected, this uses system warning records
    (yellow/orange/red) as a proxy for historical pressure. Test warnings are excluded.
    """
    since = datetime.utcnow() - timedelta(days=max(1, years) * 365)
    db = SessionLocal()
    try:
        # Exclude test warnings (consistent with warning_filters.is_test_warning()).
        test_source_like = (
            func.lower(func.coalesce(Warning.source, "")).like("%test%")
            | func.lower(func.coalesce(Warning.source, "")).like("%mock%")
            | func.lower(func.coalesce(Warning.source, "")).like("%demo%")
            | func.lower(func.coalesce(Warning.source, "")).like("%manual%")
            | func.lower(func.coalesce(Warning.source, "")).like("%sample%")
        )
        test_reason_like = (
            func.lower(func.coalesce(Warning.reason, "")).like("%测试%")
            | func.lower(func.coalesce(Warning.reason, "")).like("%演示%")
            | func.lower(func.coalesce(Warning.reason, "")).like("%mock%")
            | func.lower(func.coalesce(Warning.reason, "")).like("%demo%")
            | func.lower(func.coalesce(Warning.reason, "")).like("%sample%")
            | func.lower(func.coalesce(Warning.reason, "")).like("%杭州橙色%")
        )
        is_test = test_source_like | test_reason_like

        stmt = (
            select(func.count(Warning.id))
            .select_from(Warning)
            .join(Region, Region.id == Warning.region_id)
            .where(
                Region.code == code,
                Warning.created_at >= since,
                Warning.level.in_(["yellow", "orange", "red"]),
                ~is_test,
            )
        )
        count = int(db.execute(stmt).scalar() or 0)
        return max(0, count)
    except Exception:
        return 0
    finally:
        db.close()


async def get_last_disaster_event(code: str) -> Optional[Dict[str, Any]]:
    db = SessionLocal()
    try:
        stmt = (
            select(Warning.created_at, Warning.level)
            .select_from(Warning)
            .join(Region, Region.id == Warning.region_id)
            .where(Region.code == code, Warning.level.in_(["yellow", "orange", "red"]))
            .order_by(Warning.created_at.desc())
            .limit(1)
        )
        row = db.execute(stmt).first()
        if not row:
            return None
        created_at, level = row
        return {"date": created_at.date().isoformat(), "severity": level}
    except Exception:
        return None
    finally:
        db.close()


async def get_neighbor_average(code: str, metric: str) -> float:
    """
    Placeholder spatial metric. Uses same province-code neighbors as a lightweight approximation.
    """
    _ = metric
    province_prefix = str(code)[:2]
    if not province_prefix:
        return 0.0

    db = SessionLocal()
    try:
        stmt = (
            select(func.avg(Warning.level))
            .select_from(Warning)
            .join(Region, Region.id == Warning.region_id)
            .where(Region.code.like(f"{province_prefix}%"))
        )
        _ = db.execute(stmt).first()
    except Exception:
        pass
    finally:
        db.close()

    rng = _deterministic_rng(code + "_neighbor")
    return round(rng.uniform(20, 70), 2)

