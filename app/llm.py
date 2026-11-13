"""LLM abstraction.

Two providers share one async streaming interface:

* ``mock`` (default): fully deterministic, offline, no API key.  The generated
  text is derived from structured markers the orchestrator puts in the prompt,
  so the same prompts are meaningful to a real model.
* ``openai``: any OpenAI-compatible ``/chat/completions`` endpoint, streamed
  with httpx.  Cancelling the orchestrator task propagates into
  ``response.aiter_lines()`` and the HTTP stream is closed by the context
  manager.

Prompts use ``[ROLE:<name>]`` markers in the system message only so the mock
provider can pick a response template without a second model call.
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any, AsyncIterator, Dict, List

from .config import settings

ROLE_RE = re.compile(r"\[ROLE:([a-z_]+)\]")


class LLMError(RuntimeError):
    pass


def role_of(messages: List[Dict[str, str]]) -> str:
    for message in messages:
        if message.get("role") == "system":
            match = ROLE_RE.search(message.get("content", ""))
            if match:
                return match.group(1)
    return "unknown"


def last_user(messages: List[Dict[str, str]]) -> str:
    for message in reversed(messages):
        if message.get("role") == "user":
            return message.get("content", "")
    return ""


def extract_line(prompt: str, marker: str) -> str:
    match = re.search(rf"^{re.escape(marker)}:\s*(.*)$", prompt, re.MULTILINE)
    return match.group(1).strip() if match else ""


def extract_json(prompt: str, marker: str, default: Any) -> Any:
    match = re.search(rf"^{re.escape(marker)}:\s*(\{{.*\}}|\[.*\])$", prompt, re.MULTILINE)
    if not match:
        return default
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return default


class BaseLLM:
    name = "base"
    model = ""

    async def stream(
        self,
        messages: List[Dict[str, str]],
        *,
        max_tokens: int = 400,
        temperature: float = 0.2,
    ) -> AsyncIterator[str]:
        raise NotImplementedError
        yield ""  # pragma: no cover - makes this an async generator


# --- deterministic mock ---------------------------------------------------

def mock_plan(prompt: str) -> str:
    query = extract_line(prompt, "USER_QUESTION") or "the user question"
    angles = [
        "core concepts and definitions",
        "mechanisms, architecture and implementation",
        "trade-offs, evaluation and limitations",
    ]
    count = settings.researcher_count
    while len(angles) < count:
        angles.append(f"additional angle {len(angles) + 1}")
    steps = [
        {
            "id": f"researcher-{index + 1}",
            "question": f"{query} (focus: {angle})",
            "focus": angle,
        }
        for index, angle in enumerate(angles[:count])
    ]
    return json.dumps({"steps": steps}, ensure_ascii=False)


def mock_research(prompt: str) -> str:
    question = extract_line(prompt, "RESEARCH_QUESTION") or "the research question"
    sources = extract_json(prompt, "SOURCES_JSON", [])
    lines = [
        f"Corpus search returned {len(sources)} candidate source(s) for: {question}",
        "",
    ]
    for index, source in enumerate(sources, start=1):
        title = str(source.get("title") or f"source {index}")
        snippet = str(source.get("snippet") or "").strip()
        lines.append(f"- {title}: {snippet[:190]}")
    primary = sources[0] if sources else None
    if primary:
        title = str(primary.get("title") or "the primary source")
        content = str(primary.get("content") or primary.get("snippet") or "").strip()
        lines.append("")
        lines.append(f"Primary evidence from '{title}':")
        lines.append(content[:520])
        lines.append("")
        lines.append(
            f"The retrieved material on {question} is consistent across the "
            f"selected sources and is used below as cited evidence."
        )
    return "\n".join(lines)


def mock_write(prompt: str) -> str:
    question = extract_line(prompt, "USER_QUESTION") or "Research report"
    sources = extract_json(prompt, "SOURCES_JSON", [])
    notes_chars = len(prompt)

    title = question.rstrip(" ?.!").strip()[:110] or "Research report"
    lines = [f"# {title}", ""]
    lines.append("## Summary")
    lines.append(
        f"This report combines {len(sources)} retrieved sources across parallel "
        f"research threads into one answer. The merged context was {notes_chars} "
        f"characters after budgeting."
    )
    lines.append("")
    lines.append("## Key findings")
    for index, source in enumerate(sources[:4], start=1):
        source_title = str(source.get("title") or f"Source {index}")
        snippet = str(source.get("snippet") or "").strip()
        lines.append(f"- **{source_title}** - {snippet[:170]} [{index}]")
    if not sources:
        lines.append("- No external sources were available; this is an unsourced outline.")
    lines.append("")
    lines.append("## Synthesis")
    if sources:
        lines.append(
            "Across the evidence, the recurring theme is that the behavior described "
            "in the sources is structural rather than incidental: the design of the "
            "surrounding system constrains what the model can do, and observable "
            "state makes those constraints verifiable [1]."
        )
        if len(sources) > 1:
            lines.append(
                "The remaining sources add implementation detail and caveats, "
                "especially around failure handling and evaluation [2]."
            )
    else:
        lines.append("No synthesis is possible without retrieved evidence.")
    lines.append("")
    lines.append("## Takeaways")
    if sources:
        lines.append(f"1. Ground every claim in a numbered source; this run cites {len(sources)} of them.")
        lines.append("2. Keep orchestration state observable so failures and latency are inspectable [1].")
        lines.append("3. Treat the answer as a draft until a human checks the cited evidence.")
    else:
        lines.append("1. Provide a corpus or network access before trusting the output.")
    return "\n".join(lines)


class MockLLM(BaseLLM):
    name = "mock"
    model = "mock-deterministic-v1"
    _ROLE_SPEED = {"planner": 1.2, "researcher": 1.0, "writer": 0.9}

    def __init__(self, delay: float | None = None, chunk_chars: int | None = None) -> None:
        self.delay = settings.mock_token_delay if delay is None else delay
        self.chunk_chars = max(8, settings.mock_token_chars if chunk_chars is None else chunk_chars)

    async def stream(
        self,
        messages: List[Dict[str, str]],
        *,
        max_tokens: int = 400,
        temperature: float = 0.2,
    ) -> AsyncIterator[str]:
        role = role_of(messages)
        prompt = last_user(messages)
        if role == "planner":
            text = mock_plan(prompt)
        elif role == "researcher":
            text = mock_research(prompt)
        elif role == "writer":
            text = mock_write(prompt)
        else:
            text = prompt.strip()[:400] or "OK"
        text = text[: max_tokens * 4]
        speed = self._ROLE_SPEED.get(role, 1.0)
        # NB: the delay must stay above the Windows asyncio timer granularity
        # (~15.6 ms); sub-tick sleeps can be serviced immediately, which would
        # make the "stream" arrive in one burst.
        delay = max(0.02, self.delay * speed)
        for offset in range(0, len(text), self.chunk_chars):
            # asyncio.sleep is a real await point: task.cancel() lands here.
            await asyncio.sleep(delay)
            yield text[offset : offset + self.chunk_chars]


# --- OpenAI-compatible ----------------------------------------------------

class OpenAICompatibleLLM(BaseLLM):
    name = "openai-compatible"

    def __init__(self, base_url: str, api_key: str, model: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model

    async def stream(
        self,
        messages: List[Dict[str, str]],
        *,
        max_tokens: int = 400,
        temperature: float = 0.2,
    ) -> AsyncIterator[str]:
        import httpx

        url = f"{self.base_url}/chat/completions"
        payload = {
            "model": self.model,
            "messages": messages,
            "stream": True,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        timeout = httpx.Timeout(connect=10.0, read=180.0, write=30.0, pool=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("POST", url, json=payload, headers=headers) as response:
                if response.status_code >= 400:
                    body = (await response.aread()).decode("utf-8", "replace")
                    raise LLMError(f"LLM HTTP {response.status_code}: {body[:500]}")
                async for line in response.aiter_lines():
                    if not line or not line.startswith("data:"):
                        continue
                    data = line[5:].strip()
                    if data == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data)
                    except json.JSONDecodeError:
                        continue
                    choices = chunk.get("choices") or []
                    if not choices:
                        continue
                    delta = choices[0].get("delta") or {}
                    content = delta.get("content")
                    if content:
                        yield content


_llm: BaseLLM | None = None


def build_llm() -> BaseLLM:
    global _llm
    if _llm is not None:
        return _llm
    provider = settings.llm_provider
    if provider in ("openai", "openai-compatible", "openai_compatible", "compatible"):
        _llm = OpenAICompatibleLLM(
            base_url=settings.llm_base_url,
            api_key=settings.llm_api_key,
            model=settings.llm_model,
        )
    else:
        _llm = MockLLM()
    return _llm


def llm_info() -> Dict[str, Any]:
    client = build_llm()
    return {
        "provider": client.name,
        "model": client.model,
        "base_url": settings.llm_base_url if client.name != "mock" else None,
        "offline": settings.offline,
    }
