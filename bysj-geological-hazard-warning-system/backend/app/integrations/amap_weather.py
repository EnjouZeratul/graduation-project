from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class AMapLiveWeather:
    adcode: str
    city: str
    province: str
    weather: str
    temperature_c: Optional[float]
    humidity_pct: Optional[float]
    wind_direction: str
    wind_power_level: Optional[int]
    report_time: str


def parse_amap_live(payload: Dict[str, Any]) -> Tuple[Optional[AMapLiveWeather], Optional[str]]:
    """
    Parse AMap weatherInfo response (extensions=base).
    Returns (live, error_message).
    """
    try:
        if str(payload.get("status")) != "1":
            return None, str(payload.get("info") or "amap_status_not_1")
        if str(payload.get("infocode")) != "10000":
            return None, str(payload.get("infocode") or "amap_infocode_not_10000")
        lives = payload.get("lives") or []
        if not isinstance(lives, list) or not lives:
            return None, "amap_no_lives"
        item = lives[0] if isinstance(lives[0], dict) else None
        if not item:
            return None, "amap_invalid_lives_item"

        def _f(v: Any) -> Optional[float]:
            if v is None:
                return None
            s = str(v).strip()
            if not s:
                return None
            try:
                return float(s)
            except Exception:
                return None

        return (
            AMapLiveWeather(
                province=str(item.get("province") or ""),
                city=str(item.get("city") or ""),
                adcode=str(item.get("adcode") or ""),
                weather=str(item.get("weather") or ""),
                temperature_c=_f(item.get("temperature_float") or item.get("temperature")),
                humidity_pct=_f(item.get("humidity_float") or item.get("humidity")),
                wind_direction=str(item.get("winddirection") or ""),
                wind_power_level=parse_wind_power_level(item.get("windpower")),
                report_time=str(item.get("reporttime") or ""),
            ),
            None,
        )
    except Exception as exc:
        return None, f"amap_parse_error:{str(exc)[:160]}"


def parse_wind_power_level(value: Any) -> Optional[int]:
    """
    AMap windpower examples: '≤3', '4', '5', ... '12'
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    m = re.search(r"(\\d+)", text)
    if not m:
        return None
    try:
        lvl = int(m.group(1))
        return max(0, min(12, lvl))
    except Exception:
        return None


def wind_level_to_speed_ms(level: Optional[int]) -> Optional[float]:
    """
    Rough mapping of wind power level (1-12) to wind speed (m/s).
    This is an approximation for risk scoring, not a metrological conversion.
    """
    if level is None:
        return None
    mapping = {
        0: 0.0,
        1: 1.5,
        2: 2.5,
        3: 4.0,
        4: 5.5,
        5: 7.9,
        6: 10.8,
        7: 13.9,
        8: 17.2,
        9: 20.8,
        10: 24.5,
        11: 28.5,
        12: 32.7,
    }
    return mapping.get(int(level))


def estimate_rain_from_weather_text(weather_text: str) -> Tuple[Optional[float], Optional[float], str]:
    """
    AMap live weather does not provide precipitation (mm).
    To keep the system usable when AMap is the only available source, we estimate rain_1h/rain_24h
    from the phenomenon text. This is heuristic and should be treated as low-confidence.

    Returns (rain_1h_mm, rain_24h_mm, note)
    """
    text = str(weather_text or "").strip()
    if not text:
        return None, None, "amap_weather_text_missing"

    # default: no rain
    rain_1h = 0.0
    rain_24h = 0.0
    note = "根据天气现象估算降雨"

    # Snow/ice: treat as low liquid precipitation; keep minimal values to avoid false heavy-rain signal.
    if any(k in text for k in ["雪", "雨夹雪", "冻雨"]):
        return 1.0, 8.0, note

    if "毛毛雨" in text or "细雨" in text:
        return 0.5, 2.0, note

    # Ranges
    if "小雨-中雨" in text:
        return 3.0, 18.0, note
    if "中雨-大雨" in text:
        return 7.0, 35.0, note
    if "大雨-暴雨" in text:
        return 14.0, 70.0, note
    if "暴雨-大暴雨" in text:
        return 26.0, 140.0, note
    if "大暴雨-特大暴雨" in text:
        return 38.0, 260.0, note

    # Specific intensities
    if "特大暴雨" in text:
        return 45.0, 320.0, note
    if "大暴雨" in text:
        return 35.0, 220.0, note
    if "暴雨" in text:
        return 25.0, 140.0, note
    if "大雨" in text:
        return 15.0, 80.0, note
    if "中雨" in text:
        return 7.0, 40.0, note
    if "小雨" in text:
        return 3.0, 18.0, note

    if "强雷阵雨" in text or "强阵雨" in text:
        return 25.0, 120.0, note
    if "雷阵雨" in text or "阵雨" in text:
        return 12.0, 55.0, note
    if "极端降雨" in text:
        return 55.0, 360.0, note
    if "雨" in text:
        return 6.0, 25.0, note

    return rain_1h, rain_24h, "无降雨或未识别为降雨"

