from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional
from urllib.parse import urlparse

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    BeautifulSoup = None

_NUMBER = r"([0-9]+(?:\.[0-9]+)?)"


def _source_host(raw_data: Dict[str, Any]) -> str:
    source_url = str(raw_data.get("_source_url") or "").strip()
    return (urlparse(source_url).hostname or "").lower()


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _strip_html(raw_html: str) -> str:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", raw_html)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    return re.sub(r"\s+", " ", text).strip()


def _focus_window(text: str, anchors: list[str], radius: int = 700) -> str:
    if not text:
        return ""
    for anchor in anchors:
        idx = text.find(anchor)
        if idx >= 0:
            start = max(0, idx - radius)
            end = min(len(text), idx + radius)
            return text[start:end]
    return ""


def _extract_by_patterns(text: str, patterns: list[str]) -> Optional[float]:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        value = _to_float(match.group(1))
        if value is not None:
            return value
    return None


def _extract_json_candidates(raw_text: str) -> list[Dict[str, Any]]:
    candidates: list[Dict[str, Any]] = []

    script_pattern = re.compile(
        r"(?is)<script[^>]*?(?:application/ld\+json|application/json)[^>]*>(.*?)</script>"
    )
    for match in script_pattern.findall(raw_text):
        snippet = str(match).strip()
        if not snippet:
            continue
        try:
            parsed = json.loads(snippet)
            if isinstance(parsed, dict):
                candidates.append(parsed)
            elif isinstance(parsed, list):
                candidates.extend([item for item in parsed if isinstance(item, dict)])
        except Exception:
            continue

    return candidates


def _json_find_number(payload: Any, keys: set[str]) -> Optional[float]:
    if isinstance(payload, dict):
        for key, value in payload.items():
            key_l = str(key).lower()
            if key_l in keys:
                fv = _to_float(value)
                if fv is not None:
                    return fv
            found = _json_find_number(value, keys)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _json_find_number(item, keys)
            if found is not None:
                return found
    return None


def _json_find_text(payload: Any, keys: set[str]) -> Optional[str]:
    if isinstance(payload, dict):
        for key, value in payload.items():
            key_l = str(key).lower()
            if key_l in keys and value not in (None, ""):
                return str(value)
            found = _json_find_text(value, keys)
            if found:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _json_find_text(item, keys)
            if found:
                return found
    return None


def _parse_wttr_weather(raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # wttr.in style JSON
    current = raw_data.get("current_condition")
    weather = raw_data.get("weather")
    if not isinstance(current, list) or not current:
        return None

    c0 = current[0] if isinstance(current[0], dict) else {}
    humidity = _to_float(c0.get("humidity"))
    wind_kmph = _to_float(c0.get("windspeedKmph"))
    wind_ms = (wind_kmph or 0.0) / 3.6

    rain_1h = _to_float(c0.get("precipMM"))
    rain_24h = 0.0

    if isinstance(weather, list) and weather and isinstance(weather[0], dict):
        hourly = weather[0].get("hourly")
        if isinstance(hourly, list):
            hourly_precip: list[float] = []
            for item in hourly:
                if not isinstance(item, dict):
                    continue
                p = _to_float(item.get("precipMM"))
                if p is not None:
                    hourly_precip.append(p)
            if hourly_precip:
                rain_24h = float(sum(hourly_precip))
                if rain_1h is None:
                    # wttr hourly granularity is usually 3h; normalize to 1h.
                    rain_1h = float(max(hourly_precip)) / 3.0

    return {
        "rain_24h": round(float(rain_24h), 2),
        "rain_1h": round(float(rain_1h or 0.0), 2),
        "wind_speed": round(float(wind_ms), 2),
        "humidity": round(float(humidity or 0.0), 1),
        "data_mode": "scraped",
    }


def _parse_weather_from_text(raw_text: str) -> Dict[str, Any]:
    text = _strip_html(raw_text)

    rain_24h = _extract_by_patterns(
        text,
        [
            rf"(?:24\s*h|24\s*hour|24\s*hours)[^0-9]{{0,16}}{_NUMBER}\s*(?:mm|millimeter)",
            rf"(?:24\u5c0f\u65f6(?:\u964d\u6c34|\u964d\u96e8)?|\u65e5\u964d\u96e8\u91cf)[^0-9]{{0,16}}{_NUMBER}\s*(?:mm|\u6beb\u7c73)?",
            rf"(?:\u964d\u6c34\u91cf|\u96e8\u91cf)[^0-9]{{0,16}}{_NUMBER}\s*(?:mm|\u6beb\u7c73)",
        ],
    )
    rain_1h = _extract_by_patterns(
        text,
        [
            rf"(?:1\s*h|1\s*hour)[^0-9]{{0,16}}{_NUMBER}\s*(?:mm|millimeter)",
            rf"(?:1\u5c0f\u65f6(?:\u964d\u6c34|\u964d\u96e8)?|\u5c0f\u65f6\u96e8\u91cf)[^0-9]{{0,16}}{_NUMBER}\s*(?:mm|\u6beb\u7c73)?",
        ],
    )
    wind_speed = _extract_by_patterns(
        text,
        [
            rf"(?:wind\s*speed)[^0-9]{{0,16}}{_NUMBER}\s*(?:m/s|mps|km/h|kmh)",
            rf"(?:\u98ce\u901f|\u5e73\u5747\u98ce\u901f)[^0-9]{{0,16}}{_NUMBER}\s*(?:m/s|\u7c73/\u79d2|km/h)?",
        ],
    )
    humidity = _extract_by_patterns(
        text,
        [
            rf"(?:humidity|relative\s*humidity)[^0-9]{{0,16}}{_NUMBER}\s*%",
            rf"(?:\u6e7f\u5ea6|\u76f8\u5bf9\u6e7f\u5ea6)[^0-9]{{0,16}}{_NUMBER}\s*%",
        ],
    )

    if all(v is None for v in [rain_24h, rain_1h, wind_speed, humidity]):
        return {"error": "unsupported_scraper_payload", "message": "html_parse_no_metrics"}

    return {
        "rain_24h": float(rain_24h or 0.0),
        "rain_1h": float(rain_1h or 0.0),
        "wind_speed": float(wind_speed or 0.0),
        "humidity": float(humidity or 0.0),
        "data_mode": "scraped",
    }


def _beaufort_to_mps(level: int) -> float:
    table = {
        0: 0.2,
        1: 1.5,
        2: 3.3,
        3: 5.4,
        4: 7.9,
        5: 10.7,
        6: 13.8,
        7: 17.1,
        8: 20.7,
        9: 24.4,
        10: 28.4,
        11: 32.6,
        12: 36.9,
    }
    return table.get(max(0, min(12, level)), 0.0)


def _estimate_rain_from_condition(text: str) -> tuple[float, float]:
    if not text:
        return 0.0, 0.0
    candidates = [
        ("特大暴雨", 250.0, 35.0),
        ("大暴雨", 180.0, 25.0),
        ("暴雨", 100.0, 15.0),
        ("大雨", 50.0, 8.0),
        ("中雨", 22.0, 3.6),
        ("小雨", 8.0, 1.1),
        ("雷阵雨", 16.0, 2.4),
        ("阵雨", 9.0, 1.2),
        ("雨夹雪", 6.0, 0.7),
        ("冻雨", 8.0, 1.0),
        ("小雪", 1.8, 0.2),
        ("中雪", 4.0, 0.4),
        ("大雪", 8.0, 0.8),
        ("暴雪", 15.0, 1.5),
    ]
    rain_24h = 0.0
    rain_1h = 0.0
    for token, mm24, mm1 in candidates:
        if token in text:
            rain_24h = max(rain_24h, mm24)
            rain_1h = max(rain_1h, mm1)
    return rain_24h, rain_1h


def _extract_tianqi_week_snapshot(raw_html: str) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {
        "condition_text": "",
        "wind_level": None,
        "wind_speed": None,
    }
    if not raw_html:
        return snapshot

    if BeautifulSoup is not None:
        try:
            soup = BeautifulSoup(raw_html, "html.parser")
            week_box = soup.find("div", class_=lambda v: isinstance(v, str) and "week" in v)
            if week_box is not None:
                condition_tokens: list[str] = []
                wind_levels: list[int] = []
                for li in week_box.find_all("li"):
                    wea = li.find("span", class_=lambda v: isinstance(v, str) and "wea" in v)
                    if wea and wea.get_text(strip=True):
                        condition_tokens.append(wea.get_text(strip=True))
                    for win in li.find_all("span", class_=lambda v: isinstance(v, str) and "win" in v):
                        text = win.get_text(strip=True)
                        for m in re.findall(r"([0-9]{1,2})\s*级", text):
                            try:
                                wind_levels.append(max(0, min(12, int(m))))
                            except Exception:
                                continue
                if condition_tokens:
                    snapshot["condition_text"] = " ".join(condition_tokens)
                if wind_levels:
                    level = max(wind_levels)
                    snapshot["wind_level"] = float(level)
                    snapshot["wind_speed"] = _beaufort_to_mps(level)
                if snapshot["condition_text"] or snapshot["wind_speed"] is not None:
                    return snapshot
        except Exception:
            pass

    week_match = re.search(
        r'(?is)<div[^>]*class=["\'][^"\']*week[^"\']*["\'][^>]*>(.*?)</div>',
        raw_html,
    )
    container = week_match.group(1) if week_match else raw_html

    condition_nodes = re.findall(
        r'(?is)<span[^>]*class=["\'][^"\']*(?:wea|weather)[^"\']*["\'][^>]*>(.*?)</span>',
        container,
    )
    if not condition_nodes:
        condition_nodes = re.findall(r'(?is)"(?:wea|weather)"\s*:\s*"([^"]{1,20})"', raw_html)

    conditions: list[str] = []
    for node in condition_nodes:
        token = _strip_html(str(node)).strip()
        if token:
            conditions.append(token)
    snapshot["condition_text"] = " ".join(conditions)

    wind_nodes = re.findall(
        r'(?is)<span[^>]*class=["\'][^"\']*(?:win|wind)[^"\']*["\'][^>]*>(.*?)</span>',
        container,
    )
    wind_levels: list[int] = []
    for node in wind_nodes:
        text = _strip_html(str(node))
        for m in re.findall(r"([0-9]{1,2})\s*级", text):
            try:
                wind_levels.append(max(0, min(12, int(m))))
            except Exception:
                continue
    if wind_levels:
        level = max(wind_levels)
        snapshot["wind_level"] = float(level)
        snapshot["wind_speed"] = _beaufort_to_mps(level)

    return snapshot


def _sanitize_weather_output(result: Dict[str, Any]) -> Dict[str, Any]:
    rain_24h = float(result.get("rain_24h", 0) or 0)
    rain_1h = float(result.get("rain_1h", 0) or 0)
    wind_speed = float(result.get("wind_speed", 0) or 0)
    humidity = float(result.get("humidity", 0) or 0)

    # Avoid storing "all-zero" payloads as if they were valid observations.
    if rain_24h <= 0 and rain_1h <= 0 and wind_speed <= 0 and humidity <= 0:
        return {"error": "unsupported_scraper_payload", "message": "all_zero_metrics"}
    return result


def _parse_tianqi_weather_page(raw_text: str) -> Dict[str, Any]:
    text = _strip_html(raw_text)
    week_snapshot = _extract_tianqi_week_snapshot(raw_text)
    # Prefer extraction around weather modules, not just page header/navigation.
    focused_main = _focus_window(
        text,
        anchors=["湿度", "风向", "空气质量", "日出", "24小时天气", "一周天气", "天气"],
        radius=1000,
    )
    focused_week = _focus_window(
        text,
        anchors=["一周天气", "15天天气", "24小时天气"],
        radius=1400,
    )
    focused = " ".join([s for s in [focused_main, focused_week, text[:1200]] if s]).strip()
    condition_hint = str(week_snapshot.get("condition_text") or "")
    focused_all = " ".join([focused, condition_hint]).strip()

    humidity = _extract_by_patterns(
        f"{focused} {text}",
        [
            rf"(?:\u6e7f\u5ea6|\u76f8\u5bf9\u6e7f\u5ea6)[^0-9]{{0,12}}{_NUMBER}\s*%",
        ],
    )
    wind_level = _extract_by_patterns(
        focused,
        [
            rf"(?:\u98ce\u5411|风力|风速)?[^0-9]{{0,12}}{_NUMBER}\s*\u7ea7",
        ],
    )
    wind_speed = _extract_by_patterns(
        focused,
        [
            rf"(?:\u98ce\u901f)[^0-9]{{0,12}}{_NUMBER}\s*(?:m/s|\u7c73/\u79d2)",
        ],
    )

    if wind_speed is None and week_snapshot.get("wind_speed") is not None:
        wind_speed = _to_float(week_snapshot.get("wind_speed"))
    if wind_level is None and week_snapshot.get("wind_level") is not None:
        wind_level = _to_float(week_snapshot.get("wind_level"))
    if wind_speed is None and wind_level is not None:
        wind_speed = _beaufort_to_mps(int(max(0, round(wind_level))))

    explicit_rain_24h = _extract_by_patterns(
        focused,
        [
            rf"(?:\u964d\u6c34\u91cf|\u7d2f\u8ba1\u964d\u6c34|\u8fc724\u5c0f\u65f6\u964d\u6c34)[^0-9]{{0,16}}{_NUMBER}\s*(?:mm|\u6beb\u7c73)?",
        ],
    )
    explicit_rain_1h = _extract_by_patterns(
        focused,
        [
            rf"(?:\u5c0f\u65f6\u964d\u6c34|\u5c0f\u65f6\u96e8\u91cf)[^0-9]{{0,16}}{_NUMBER}\s*(?:mm|\u6beb\u7c73)?",
        ],
    )
    cond_r24, cond_r1 = _estimate_rain_from_condition(focused_all)

    rain_24h = float(explicit_rain_24h if explicit_rain_24h is not None else cond_r24)
    rain_1h = float(explicit_rain_1h if explicit_rain_1h is not None else cond_r1)

    if any(v is not None for v in [humidity, wind_speed]) or rain_24h > 0 or rain_1h > 0:
        return _sanitize_weather_output(
            {
            "rain_24h": round(float(rain_24h), 2),
            "rain_1h": round(float(rain_1h), 2),
            "wind_speed": round(float(wind_speed or 0.0), 2),
            "humidity": round(float(humidity or 0.0), 1),
            "data_mode": "scraped",
            }
        )
    parsed = _parse_weather_from_text(raw_text)
    if "error" in parsed:
        return parsed
    return _sanitize_weather_output(parsed)


def _parse_weather_com_cn_page(raw_text: str) -> Dict[str, Any]:
    text = _strip_html(raw_text)
    rain_1h = _extract_by_patterns(
        text,
        [
            rf"(?:\u5c0f\u65f6\u96e8\u91cf|\u5f53\u524d\u964d\u6c34)[^0-9]{{0,16}}{_NUMBER}\s*(?:mm|\u6beb\u7c73)?",
        ],
    )
    if rain_1h is not None:
        return {
            "rain_24h": 0.0,
            "rain_1h": float(rain_1h),
            "wind_speed": 0.0,
            "humidity": 0.0,
            "data_mode": "scraped",
        }
    return _parse_weather_from_text(raw_text)


def _parse_qweather_page(raw_text: str) -> Dict[str, Any]:
    return _parse_weather_from_text(raw_text)


def parse_weather_payload(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    if "error" in raw_data:
        return raw_data

    # Priority 1: direct structured fields from upstream API/scraper JSON.
    direct_24h = raw_data.get("precipitation_24h", raw_data.get("rain_24h"))
    direct_1h = raw_data.get("precipitation_1h", raw_data.get("rain_1h"))
    direct_wind = raw_data.get("wind_speed")
    direct_humidity = raw_data.get("humidity", raw_data.get("relative_humidity"))
    if any(v is not None for v in [direct_24h, direct_1h, direct_wind, direct_humidity]):
        return _sanitize_weather_output(
            {
            "rain_24h": float(_to_float(direct_24h) or 0.0),
            "rain_1h": float(_to_float(direct_1h) or 0.0),
            "wind_speed": float(_to_float(direct_wind) or 0.0),
            "humidity": float(_to_float(direct_humidity) or 0.0),
            "data_mode": "scraped",
            }
        )

    # Priority 2: known JSON structures (wttr, embedded script json).
    wttr = _parse_wttr_weather(raw_data)
    if wttr:
        return wttr

    raw_text = raw_data.get("raw_text")
    if not isinstance(raw_text, str) or not raw_text.strip():
        return {"error": "unsupported_scraper_payload", "message": "empty_payload"}

    for obj in _extract_json_candidates(raw_text):
        rain_24h = _json_find_number(
            obj,
            {
                "rain_24h",
                "precipitation_24h",
                "precip24h",
                "dailyrain",
                "precipmm",
            },
        )
        rain_1h = _json_find_number(obj, {"rain_1h", "precipitation_1h", "precip1h"})
        humidity = _json_find_number(obj, {"humidity", "relative_humidity", "rh"})
        wind_speed = _json_find_number(obj, {"wind_speed", "windspeed", "windspeedkmph"})

        if any(v is not None for v in [rain_24h, rain_1h, humidity, wind_speed]):
            if wind_speed is not None and wind_speed > 35:
                # Commonly km/h from some weather feeds.
                wind_speed = wind_speed / 3.6
            return _sanitize_weather_output(
                {
                "rain_24h": float(rain_24h or 0.0),
                "rain_1h": float(rain_1h or 0.0),
                "wind_speed": float(wind_speed or 0.0),
                "humidity": float(humidity or 0.0),
                "data_mode": "scraped",
                }
            )

    # Priority 3: domain-specific routing then generic regex fallback.
    host = _source_host(raw_data)
    if host.endswith("wttr.in"):
        parsed = _parse_weather_from_text(raw_text)
        if "error" not in parsed:
            return parsed
        return {"error": "unsupported_scraper_payload", "message": "wttr_payload_unrecognized"}
    if host.endswith("tianqi.com"):
        return _parse_tianqi_weather_page(raw_text)
    if host.endswith("weather.com.cn"):
        return _parse_weather_com_cn_page(raw_text)
    if host.endswith("qweather.com"):
        return _parse_qweather_page(raw_text)

    parsed = _parse_weather_from_text(raw_text)
    if "error" in parsed:
        return parsed
    return _sanitize_weather_output(parsed)


def _parse_geology_from_text(raw_text: str) -> Dict[str, Any]:
    text = _strip_html(raw_text)

    slope = _extract_by_patterns(
        text,
        [
            rf"(?:slope|slope\s*angle)[^0-9]{{0,16}}{_NUMBER}\s*(?:deg|degree|\u00b0)",
            rf"(?:\u5761\u5ea6|\u5761\u89d2)[^0-9]{{0,16}}{_NUMBER}\s*(?:\u00b0|\u5ea6)?",
        ],
    )
    fault_distance = _extract_by_patterns(
        text,
        [
            rf"(?:fault\s*distance|distance\s*to\s*fault)[^0-9]{{0,16}}{_NUMBER}\s*(?:km|kilometer)",
            rf"(?:\u65ad\u5c42\u8ddd\u79bb|\u8ddd\u79bb\u65ad\u5c42)[^0-9]{{0,16}}{_NUMBER}\s*(?:km|\u516c\u91cc)?",
        ],
    )

    lithology = "unknown"
    lithology_map = {
        "granite": "granite",
        "sandstone": "sandstone",
        "shale": "shale",
        "limestone": "limestone",
        "\u82b1\u5c97\u5ca9": "granite",
        "\u7802\u5ca9": "sandstone",
        "\u9875\u5ca9": "shale",
        "\u77f3\u7070\u5ca9": "limestone",
    }
    lower = text.lower()
    for token, mapped in lithology_map.items():
        if token.lower() in lower:
            lithology = mapped
            break

    if slope is None and fault_distance is None and lithology == "unknown":
        return {"error": "unsupported_scraper_payload", "message": "html_parse_no_metrics"}

    return {
        "slope": float(slope or 0.0),
        "fault_distance": float(fault_distance or 999.0),
        "lithology": lithology,
        "data_mode": "scraped",
    }


def parse_geology_payload(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    if "error" in raw_data:
        return raw_data

    direct_slope = raw_data.get("terrain_slope", raw_data.get("slope", raw_data.get("slope_degree")))
    direct_fault = raw_data.get("fault_distance_km", raw_data.get("fault_distance", raw_data.get("fault_km")))
    direct_lithology = raw_data.get("lithology", raw_data.get("rock_type"))
    if any(v is not None for v in [direct_slope, direct_fault, direct_lithology]):
        return {
            "slope": float(_to_float(direct_slope) or 0.0),
            "fault_distance": float(_to_float(direct_fault) or 999.0),
            "lithology": str(direct_lithology or "unknown"),
            "data_mode": "scraped",
        }

    raw_text = raw_data.get("raw_text")
    if not isinstance(raw_text, str) or not raw_text.strip():
        return {"error": "unsupported_scraper_payload", "message": "empty_payload"}

    for obj in _extract_json_candidates(raw_text):
        slope = _json_find_number(obj, {"slope", "terrain_slope", "slope_degree"})
        fault_distance = _json_find_number(obj, {"fault_distance", "fault_distance_km", "fault_km"})
        lithology = _json_find_text(obj, {"lithology", "rock_type"})
        if slope is None and fault_distance is None and not lithology:
            continue
        return {
            "slope": float(slope or 0.0),
            "fault_distance": float(fault_distance or 999.0),
            "lithology": str(lithology or "unknown"),
            "data_mode": "scraped",
        }

    return _parse_geology_from_text(raw_text)

