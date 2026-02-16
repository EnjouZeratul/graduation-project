from functools import lru_cache
from typing import List

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str
    redis_url: str

    deepseek_api_key: str = ""
    deepseek_base_url: str = "https://api.deepseek.com"
    deepseek_model: str = "deepseek-chat"
    llm_provider: str = "auto"
    llm_api_key: str = ""
    llm_base_url: str = ""
    llm_model: str = ""
    openai_api_key: str = ""
    openai_base_url: str = "https://api.openai.com/v1"
    openai_model: str = "gpt-4o-mini"
    qwen_api_key: str = ""
    qwen_base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    qwen_model: str = "qwen-plus"

    backend_host: str = "0.0.0.0"
    backend_port: int = 8000

    allowed_origins: List[str] = ["*"]

    # External API configurations
    cma_api_key: str = ""
    cma_base_url: str = "http://api.data.cma.cn"
    # CMA national surface observation API (station-based).
    # If set, this takes precedence over the placeholder `cma_api_key` live mode.
    cma_user_id: str = ""
    cma_password: str = ""
    cma_interface_id: str = "getSurfEleByTimeRangeAndStaID"
    cma_data_code: str = "SURF_CHN_MUL_HOR_3H"
    # CMA timeRange is typically interpreted in China local time; we default to UTC+8.
    cma_time_zone_offset_hours: int = 8
    cgs_api_key: str = ""
    cgs_base_url: str = "http://api.cgs.gov.cn"
    openweather_api_key: str = ""
    amap_api_key: str = ""
    amap_base_url: str = "https://restapi.amap.com"
    # Weather Underground supplemental source (via api.weather.com endpoints discovered from page payload).
    wu_enabled: bool = False
    wu_api_key: str = ""
    wu_api_base_url: str = "https://api.weather.com"
    wu_key_discovery_enabled: bool = True
    wu_key_discovery_url: str = "https://www.wunderground.com/weather/cn/hangzhou"
    wu_key_refresh_minutes: int = 360
    wu_timeout_seconds: float = 8.0
    wu_max_retries: int = 1
    wu_language: str = "en-US"
    wu_units: str = "m"
    wu_reliability: float = 0.62

    # Workflow tuning
    enable_llm_refinement: bool = True
    llm_refine_max_regions: int = 20
    llm_confidence_threshold: float = 0.6
    neighbor_influence_weight: float = 0.2

    # Scraper safety controls
    unofficial_scraper_enabled: bool = True
    scraper_allowed_domains: List[str] = []
    scraper_url_template: str = ""
    geology_scraper_enabled: bool = False
    geology_scraper_url_template: str = ""
    scraper_user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    )
    scraper_request_interval_seconds: float = 1.5
    scraper_max_parallel_requests: int = 2
    scraper_timeout_seconds: float = 8.0
    scraper_max_retries: int = 1
    scraper_max_requests_per_window: int = 30
    scraper_parser_cooldown_minutes: int = 3
    scraper_cache_minutes: int = 25
    collector_max_concurrency: int = 12
    scraper_city_level_only: bool = True
    scraper_tianqi_city_index_url: str = "https://www.tianqi.com/chinacity.html"
    scraper_tianqi_index_ttl_minutes: int = 360

    # Default higher than a single scraping run for nationwide coverage; can be lowered via .env for dev.
    workflow_max_runtime_seconds: int = 3600 
    workflow_manual_region_limit: int = 100

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_prefix = ""
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()
