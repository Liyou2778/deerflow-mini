"""Runtime configuration for deerflow-mini.

Every knob is environment-overridable so the same code can run offline (mock
LLM) or against any OpenAI-compatible endpoint without edits.
"""

from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "app" / "static"
CORPUS_DIR = BASE_DIR / "corpus"


def _env_str(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env_str(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env_str(name, str(default)))
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    value = _env_str(name, "1" if default else "0").strip().lower()
    return value in ("1", "true", "yes", "on")


class Settings:
    """Process-wide settings. Instantiated once at import time."""

    app_name = "deerflow-mini"
    version = "0.1.0"

    host: str = _env_str("HOST", "127.0.0.1")
    port: int = _env_int("PORT", 8201)

    # Event sourcing
    event_buffer_size: int = _env_int("EVENT_BUFFER_SIZE", 500)
    heartbeat_seconds: float = _env_float("SSE_HEARTBEAT_SECONDS", 10.0)

    # LLM abstraction
    llm_provider: str = _env_str("LLM_PROVIDER", "mock").strip().lower()
    llm_base_url: str = _env_str("LLM_BASE_URL", "https://api.openai.com/v1")
    llm_api_key: str = _env_str("LLM_API_KEY", "")
    llm_model: str = _env_str("LLM_MODEL", "gpt-4o-mini")
    mock_token_delay: float = _env_float("MOCK_TOKEN_DELAY", 0.025)
    mock_token_chars: int = _env_int("MOCK_TOKEN_CHARS", 56)

    # Orchestration
    researcher_count: int = _env_int("RESEARCHER_COUNT", 3)
    max_tokens_planner: int = _env_int("MAX_TOKENS_PLANNER", 320)
    max_tokens_researcher: int = _env_int("MAX_TOKENS_RESEARCHER", 320)
    max_tokens_writer: int = _env_int("MAX_TOKENS_WRITER", 700)

    # Context budget between child agents and the writer
    context_token_budget: int = _env_int("CONTEXT_TOKEN_BUDGET", 300)
    context_strategy: str = _env_str("CONTEXT_STRATEGY", "trim_head_tail")

    # Tools
    offline: bool = _env_bool("OFFLINE", True)
    fetch_timeout_seconds: float = _env_float("FETCH_TIMEOUT_SECONDS", 8.0)


settings = Settings()
