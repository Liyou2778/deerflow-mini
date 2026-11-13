"""Tool registry with permission-prefix isolation plus offline tools.

Each tool declares a permission namespace (for example ``net``).  An agent role
declares the prefixes it may use; ``invoke`` rejects calls outside those
prefixes before touching the tool.  This keeps the policy in one place instead
of relying on prompt discipline.

Tools shipped in the MVP:

* ``net.web_search`` -- keyword search over the local ``corpus/`` markdown files
* ``net.fetch_url``   -- cached page retrieval; in offline mode it resolves URLs
  back to the local corpus instead of hitting the network

Both are async so cancellation propagates through them naturally.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .config import CORPUS_DIR, settings

_WORD_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9_-]{1,}")
_STOPWORDS = {
    "the", "and", "for", "are", "but", "not", "you", "your", "with", "from",
    "into", "that", "this", "these", "those", "what", "when", "where", "which",
    "how", "why", "does", "did", "can", "could", "should", "would", "about",
    "more", "than", "then", "there", "their", "have", "has", "had", "will",
    "one", "two", "use", "using", "used", "its", "our", "out", "all", "any",
}


def tokenize(text: str) -> List[str]:
    return [word.lower() for word in _WORD_RE.findall(text)]


@dataclass
class Tool:
    name: str
    permission: str
    description: str
    handler: Callable[..., Any]
    offline_ok: bool = True


class ToolPermissionError(PermissionError):
    pass


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: Dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        self._tools[tool.name] = tool

    def get(self, name: str) -> Optional[Tool]:
        return self._tools.get(name)

    def names(self) -> List[str]:
        return sorted(self._tools)

    def allowed(self, tool: Tool, allowed_prefixes: List[str]) -> bool:
        for prefix in allowed_prefixes:
            if tool.permission == prefix or tool.permission.startswith(prefix + ":"):
                return True
        return False

    async def invoke(
        self,
        agent: str,
        allowed_prefixes: List[str],
        tool_name: str,
        arguments: Dict[str, Any],
    ) -> Dict[str, Any]:
        tool = self._tools.get(tool_name)
        if tool is None:
            return {"ok": False, "error": f"unknown tool: {tool_name}"}
        if not self.allowed(tool, allowed_prefixes):
            raise ToolPermissionError(
                f"agent '{agent}' may not call tool '{tool_name}' "
                f"(permission '{tool.permission}', allowed {allowed_prefixes})"
            )
        return await tool.handler(**arguments)


# --- corpus loading -------------------------------------------------------

@dataclass
class Document:
    doc_id: str
    title: str
    url: str
    tags: List[str]
    content: str
    snippet: str


def _parse_document(path: Path) -> Document:
    raw = path.read_text(encoding="utf-8")
    lines = raw.splitlines()
    title = lines[0].lstrip("# ").strip() if lines else path.stem
    url = ""
    tags: List[str] = []
    body_start = 0
    for index, line in enumerate(lines[1:], start=1):
        if line.startswith("Source:"):
            url = line.split(":", 1)[1].strip()
        elif line.startswith("Tags:"):
            tags = [tag.strip().lower() for tag in line.split(":", 1)[1].split(",") if tag.strip()]
        elif line.strip() == "":
            body_start = index + 1
            continue
        elif not line.startswith(("Source:", "Tags:")):
            body_start = index
            break
    body = "\n".join(lines[body_start:]).strip()
    first_para = next((p.strip() for p in body.split("\n\n") if p.strip()), "")
    snippet = " ".join(first_para.split())[:240]
    return Document(
        doc_id=path.stem,
        title=title,
        url=url or f"https://example.org/{path.stem}",
        tags=tags,
        content=body,
        snippet=snippet,
    )


class Corpus:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.documents: List[Document] = []
        self.by_url: Dict[str, Document] = {}
        self.reload()

    def reload(self) -> None:
        self.documents = []
        self.by_url = {}
        if not self.directory.exists():
            return
        for path in sorted(self.directory.glob("*.md")):
            document = _parse_document(path)
            self.documents.append(document)
            self.by_url[document.url] = document
        for document in self.documents:
            self.by_url.setdefault(f"https://example.org/{document.doc_id}", document)

    def search(self, query: str, top_k: int = 3) -> List[Dict[str, Any]]:
        terms = [term for term in tokenize(query) if term not in _STOPWORDS]
        scored: List[tuple[float, Document]] = []
        for document in self.documents:
            haystack = f"{document.title} {document.content}".lower()
            title_lower = document.title.lower()
            score = 0.0
            for term in terms:
                if term in title_lower:
                    score += 3.0
                if term in document.tags:
                    score += 2.0
                score += min(3, haystack.count(term)) * 0.5
            if score > 0:
                scored.append((score, document))
        scored.sort(key=lambda item: (-item[0], item[1].doc_id))
        selected = scored[:top_k]
        if not selected:
            selected = [(0.0, document) for document in self.documents[:top_k]]
        return [
            {
                "title": document.title,
                "url": document.url,
                "snippet": document.snippet,
                "score": round(score, 2),
            }
            for score, document in selected
        ]


corpus = Corpus(CORPUS_DIR)


# --- tool implementations -------------------------------------------------

async def web_search(query: str, top_k: int = 3) -> Dict[str, Any]:
    # Small sleep: real tools do I/O, and it gives cancellation a real await point.
    await asyncio.sleep(0.04)
    results = corpus.search(query, top_k=top_k)
    return {"ok": True, "query": query, "engine": "local-corpus", "results": results}


async def fetch_url(url: str) -> Dict[str, Any]:
    await asyncio.sleep(0.05)
    document = corpus.by_url.get(url)
    if settings.offline:
        if document is not None:
            return {
                "ok": True,
                "url": url,
                "status": "cached",
                "mode": "offline",
                "title": document.title,
                "content": document.content,
            }
        return {
            "ok": False,
            "url": url,
            "status": "offline_miss",
            "mode": "offline",
            "error": "URL is not in the local corpus; live fetching is disabled (OFFLINE=1)",
        }
    # Online mode is intentionally not exercised by the default/test setup.
    import httpx

    try:
        async with httpx.AsyncClient(timeout=settings.fetch_timeout_seconds, follow_redirects=True) as client:
            response = await client.get(url)
        return {
            "ok": response.is_success,
            "url": url,
            "status": response.status_code,
            "mode": "online",
            "content": response.text[:8000],
        }
    except Exception as exc:  # pragma: no cover - network path
        return {"ok": False, "url": url, "status": "error", "mode": "online", "error": str(exc)}


def build_registry() -> ToolRegistry:
    registry = ToolRegistry()
    registry.register(
        Tool(
            name="web_search",
            permission="net",
            description="Search the local markdown corpus and return ranked sources.",
            handler=web_search,
        )
    )
    registry.register(
        Tool(
            name="fetch_url",
            permission="net",
            description="Fetch a source URL (offline mode resolves local corpus).",
            handler=fetch_url,
        )
    )
    return registry


registry = build_registry()
