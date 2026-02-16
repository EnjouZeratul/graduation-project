from __future__ import annotations

from dataclasses import dataclass

from openai import AsyncOpenAI

from app.core.config import Settings


@dataclass
class LLMRuntime:
    provider: str
    model: str
    client: AsyncOpenAI


def _build(provider: str, api_key: str, base_url: str, model: str) -> LLMRuntime:
    return LLMRuntime(
        provider=provider,
        model=model,
        client=AsyncOpenAI(api_key=api_key, base_url=base_url),
    )


def build_llm_runtime(settings: Settings) -> LLMRuntime | None:
    provider = (settings.llm_provider or "auto").lower().strip()

    if provider == "custom" and settings.llm_api_key and settings.llm_base_url and settings.llm_model:
        return _build("custom", settings.llm_api_key, settings.llm_base_url, settings.llm_model)

    if provider == "deepseek" and settings.deepseek_api_key:
        return _build("deepseek", settings.deepseek_api_key, settings.deepseek_base_url, settings.deepseek_model)

    if provider == "openai" and settings.openai_api_key:
        return _build("openai", settings.openai_api_key, settings.openai_base_url, settings.openai_model)

    if provider == "qwen" and settings.qwen_api_key:
        return _build("qwen", settings.qwen_api_key, settings.qwen_base_url, settings.qwen_model)

    # Auto mode priority: custom -> deepseek -> openai -> qwen
    if settings.llm_api_key and settings.llm_base_url and settings.llm_model:
        return _build("custom", settings.llm_api_key, settings.llm_base_url, settings.llm_model)

    if settings.deepseek_api_key:
        return _build("deepseek", settings.deepseek_api_key, settings.deepseek_base_url, settings.deepseek_model)

    if settings.openai_api_key:
        return _build("openai", settings.openai_api_key, settings.openai_base_url, settings.openai_model)

    if settings.qwen_api_key:
        return _build("qwen", settings.qwen_api_key, settings.qwen_base_url, settings.qwen_model)

    return None
