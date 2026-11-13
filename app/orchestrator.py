"""Self-built async orchestrator: planner -> N parallel researchers -> writer.

No LangGraph / no framework: the state machine is explicit and every transition
is an event on the run's :class:`EventBus`.  Cancellation is real asyncio task
cancellation: the run task is cancelled by the API, CancelledError unwinds
through ``asyncio.gather`` (all researchers) and through the streaming LLM
generator, node cleanup events are published, and the stream ends with
``run_cancelled``.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, List, Optional

from .config import settings
from .context import estimate_tokens, fit_results
from .llm import BaseLLM, build_llm
from .store import Run, Store, store as default_store
from .tools import ToolPermissionError, registry


class Orchestrator:
    def __init__(self, store: Optional[Store] = None, llm: Optional[BaseLLM] = None) -> None:
        self.store = store or default_store
        self.llm = llm or build_llm()

    # --- lifecycle ----------------------------------------------------
    def start(self, run: Run) -> "asyncio.Task":
        run.task = asyncio.create_task(self._run(run), name=f"run:{run.id}")
        return run.task

    def force_cancel(self, run: Run) -> None:
        """Terminalise a run whose task was cancelled before it ever started.

        ``task.cancel()`` on a never-scheduled task prevents the coroutine body
        (and therefore its CancelledError handler) from running.  The API calls
        this after awaiting the task to guarantee the SSE stream still gets a
        terminal ``run_cancelled`` event.
        """
        if not run.is_terminal():
            self._handle_cancel(run)

    async def _run(self, run: Run) -> None:
        run.started_at = time.time()
        run.status = "running"
        run.bus.publish(
            "run_start",
            query=run.query,
            provider=self.llm.name,
            model=self.llm.model,
            researcher_count=settings.researcher_count,
            context_token_budget=settings.context_token_budget,
        )
        try:
            await self._execute(run)
        except asyncio.CancelledError:
            self._handle_cancel(run)
            return  # cancellation is a clean terminal state for this run
        except Exception as exc:  # noqa: BLE001 - expose failure through the stream
            run.status = "failed"
            run.error = f"{type(exc).__name__}: {exc}"
            self._finalize(run, "run_end", status="failed", error=run.error)
            return
        run.status = "done"
        self._finalize(run, "run_end", status="done")

    async def _execute(self, run: Run) -> None:
        session = self.store.get_session(run.session_id)
        history = list(session.history[-2:]) if session else []

        plan = await self._plan(run, history)
        run.metrics["plan"] = [step["id"] for step in plan]

        results = await self._research(run, plan)
        citations = self._assign_citations(run, results)
        notes = self._build_notes(run, results)
        answer = await self._write(run, history, notes, citations)
        run.answer = answer

        if session is not None:
            session.history.append({"role": "user", "content": run.query})
            session.history.append({"role": "assistant", "content": answer})
            if session.title == "New session":
                session.title = run.query.strip()[:60]

    # --- nodes --------------------------------------------------------
    def _node_start(self, run: Run, node_id: str, label: str, kind: str, parent: Optional[str]) -> Dict[str, Any]:
        node = {
            "id": node_id,
            "label": label,
            "kind": kind,
            "parent": parent,
            "status": "running",
            "started_at": time.time(),
            "ended_at": None,
            "duration_ms": None,
            "tokens_in": 0,
            "tokens_out": 0,
            "detail": "",
        }
        run.nodes[node_id] = node
        run.bus.publish(
            "node_start",
            node_id=node_id,
            label=label,
            kind=kind,
            parent=parent,
            status="running",
            started_at=node["started_at"],
        )
        return node

    def _node_end(self, run: Run, node_id: str, status: str, detail: str = "") -> None:
        node = run.nodes.get(node_id)
        if node is None:
            return
        node["ended_at"] = time.time()
        node["duration_ms"] = round((node["ended_at"] - node["started_at"]) * 1000.0, 1)
        node["status"] = status
        if detail:
            node["detail"] = detail
        run.bus.publish(
            "node_end",
            node_id=node_id,
            status=status,
            duration_ms=node["duration_ms"],
            tokens_in=node["tokens_in"],
            tokens_out=node["tokens_out"],
            detail=detail,
        )

    async def _stream_node(
        self,
        run: Run,
        node_id: str,
        messages: List[Dict[str, str]],
        *,
        channel: str,
        max_tokens: int,
    ) -> str:
        node = run.nodes[node_id]
        node["tokens_in"] = estimate_tokens("\n".join(message["content"] for message in messages))
        node["tokens_out"] = 0
        chunks: List[str] = []
        stream = self.llm.stream(messages, max_tokens=max_tokens)
        async for chunk in stream:
            chunks.append(chunk)
            node["tokens_out"] += estimate_tokens(chunk)
            if channel == "answer":
                # Keep the partial answer on the run so a reconnecting client can
                # restore mid-stream state from the run snapshot.
                run.answer += chunk
            run.bus.publish(
                "token",
                node_id=node_id,
                text=chunk,
                channel=channel,
                tokens=estimate_tokens(chunk),
            )
        text = "".join(chunks)
        node["tokens_out"] = estimate_tokens(text)
        return text

    # --- phases -------------------------------------------------------
    async def _plan(self, run: Run, history: List[Dict[str, str]]) -> List[Dict[str, str]]:
        node_id = "planner"
        self._node_start(run, node_id, "planner / decompose question", "planner", None)
        prompt = (
            f"USER_QUESTION: {run.query}\n"
            f"RECENT_TURNS_JSON: {json.dumps(history, ensure_ascii=False)}\n"
            f"RESEARCHER_COUNT: {settings.researcher_count}\n"
            "Decompose the question into focused research sub-questions. "
            'Return strict JSON: {"steps":[{"id":"researcher-1","question":"...","focus":"..."}]}'
        )
        messages = [
            {
                "role": "system",
                "content": (
                    "[ROLE:planner] You are the planner of a multi-agent research pipeline. "
                    "Split the user question into non-overlapping research angles that can run "
                    "in parallel."
                ),
            },
            {"role": "user", "content": prompt},
        ]
        try:
            raw = await self._stream_node(
                run, node_id, messages, channel="plan", max_tokens=settings.max_tokens_planner
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._node_end(run, node_id, "failed", detail=f"{type(exc).__name__}: {exc}")
            raise
        plan = self._parse_plan(run.query, raw)
        self._node_end(run, node_id, "done", detail=f"{len(plan)} research steps")
        return plan

    def _parse_plan(self, query: str, raw: str) -> List[Dict[str, str]]:
        count = max(1, settings.researcher_count)
        steps: List[Any] = []
        start, end = raw.find("{"), raw.rfind("}")
        if 0 <= start < end:
            try:
                data = json.loads(raw[start : end + 1])
                if isinstance(data, dict) and isinstance(data.get("steps"), list):
                    steps = data["steps"]
            except json.JSONDecodeError:
                steps = []
        angles = [
            "core concepts and definitions",
            "mechanisms, architecture and implementation",
            "trade-offs, evaluation and limitations",
        ]
        plan: List[Dict[str, str]] = []
        for index in range(count):
            angle = angles[index] if index < len(angles) else f"additional angle {index + 1}"
            fallback = f"{query} (focus: {angle})"
            raw_step = steps[index] if index < len(steps) and isinstance(steps[index], dict) else {}
            question = str(raw_step.get("question") or fallback).strip() or fallback
            focus = str(raw_step.get("focus") or angle).strip() or angle
            plan.append(
                {
                    "id": f"researcher-{index + 1}",
                    "question": question,
                    "focus": focus,
                }
            )
        return plan

    async def _research(self, run: Run, plan: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        tasks = [self._researcher(run, step, index) for index, step in enumerate(plan)]
        # gather propagates cancellation from the run task to every researcher
        return list(await asyncio.gather(*tasks))

    async def _researcher(self, run: Run, step: Dict[str, str], index: int) -> Dict[str, Any]:
        node_id = step["id"]
        self._node_start(
            run, node_id, f"{node_id} / {step['focus'][:48]}", "researcher", "planner"
        )
        try:
            run.bus.publish(
                "tool_call",
                node_id=node_id,
                tool="web_search",
                agent=node_id,
                args={"query": step["question"], "top_k": 3},
            )
            tool_start = time.time()
            search = await registry.invoke(
                node_id, ["net"], "web_search", {"query": step["question"], "top_k": 3}
            )
            sources = search.get("results", [])
            run.bus.publish(
                "tool_result",
                node_id=node_id,
                tool="web_search",
                ok=bool(search.get("ok")),
                duration_ms=round((time.time() - tool_start) * 1000.0, 1),
                results=sources,
            )

            fetched: Optional[Dict[str, Any]] = None
            if sources:
                run.bus.publish(
                    "tool_call",
                    node_id=node_id,
                    tool="fetch_url",
                    agent=node_id,
                    args={"url": sources[0]["url"]},
                )
                tool_start = time.time()
                fetched = await registry.invoke(
                    node_id, ["net"], "fetch_url", {"url": sources[0]["url"]}
                )
                run.bus.publish(
                    "tool_result",
                    node_id=node_id,
                    tool="fetch_url",
                    ok=bool(fetched.get("ok")),
                    duration_ms=round((time.time() - tool_start) * 1000.0, 1),
                    status=fetched.get("status"),
                    url=fetched.get("url"),
                    content_preview=str(fetched.get("content") or "")[:240],
                )

            prompt_sources = []
            for position, source in enumerate(sources):
                item = dict(source)
                if position == 0 and fetched and fetched.get("ok"):
                    item["content"] = str(fetched.get("content") or "")[:900]
                prompt_sources.append(item)

            prompt = (
                f"RESEARCH_QUESTION: {step['question']}\n"
                f"FOCUS: {step['focus']}\n"
                f"SOURCES_JSON: {json.dumps(prompt_sources, ensure_ascii=False)}\n"
                "Summarise the retrieved evidence in plain prose, naming the sources. "
                "Do not invent facts that are absent from the sources."
            )
            messages = [
                {
                    "role": "system",
                    "content": (
                        "[ROLE:researcher] You are one of several parallel researchers. "
                        "Use only the provided search/fetch evidence and produce a compact "
                        "findings summary for the writer."
                    ),
                },
                {"role": "user", "content": prompt},
            ]
            summary = await self._stream_node(
                run, node_id, messages, channel="research", max_tokens=settings.max_tokens_researcher
            )
        except asyncio.CancelledError:
            raise
        except ToolPermissionError as exc:
            self._node_end(run, node_id, "failed", detail=f"permission denied: {exc}")
            raise
        except Exception as exc:  # noqa: BLE001 - node failure is part of the state machine
            self._node_end(run, node_id, "failed", detail=f"{type(exc).__name__}: {exc}")
            raise

        self._node_end(run, node_id, "done", detail=f"{len(sources)} sources")
        return {
            "node_id": node_id,
            "step": step,
            "sources": sources,
            "fetched": fetched,
            "summary": summary,
            "duration_ms": run.nodes[node_id]["duration_ms"],
        }

    def _assign_citations(self, run: Run, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        citations: List[Dict[str, Any]] = []
        seen = set()
        for result in results:
            for source in result.get("sources", [])[:2]:
                url = str(source.get("url") or "")
                if not url or url in seen:
                    continue
                seen.add(url)
                citation = {
                    "index": len(citations) + 1,
                    "title": str(source.get("title") or url),
                    "url": url,
                    "snippet": str(source.get("snippet") or ""),
                    "researcher": result["node_id"],
                }
                citations.append(citation)
                run.bus.publish("citation", node_id=result["node_id"], **citation)
        run.citations = citations
        return citations

    def _build_notes(self, run: Run, results: List[Dict[str, Any]]) -> str:
        items = [
            {"label": result["node_id"], "text": result.get("summary", "")}
            for result in results
        ]
        fitted = fit_results(items, settings.context_token_budget, settings.context_strategy)
        run.context_budget = fitted.meta
        run.bus.publish(
            "context_trim",
            strategy=fitted.meta.get("strategy"),
            budget_tokens=fitted.meta.get("budget_tokens"),
            per_item_tokens=fitted.meta.get("per_item_tokens"),
            tokens_before=fitted.meta.get("tokens_before"),
            tokens_after=fitted.meta.get("tokens_after"),
            trimmed_items=fitted.meta.get("trimmed_items"),
            tokens_saved=fitted.meta.get("tokens_saved"),
        )
        return "\n\n".join(fitted.texts)

    async def _write(
        self,
        run: Run,
        history: List[Dict[str, str]],
        notes: str,
        citations: List[Dict[str, Any]],
    ) -> str:
        node_id = "writer"
        self._node_start(run, node_id, "writer / final report", "writer", None)
        sources_payload = [
            {
                "index": citation["index"],
                "title": citation["title"],
                "url": citation["url"],
                "snippet": citation["snippet"],
            }
            for citation in citations
        ]
        prompt = (
            f"USER_QUESTION: {run.query}\n"
            f"RECENT_TURNS_JSON: {json.dumps(history, ensure_ascii=False)}\n"
            f"RESEARCH_NOTES:\n{notes}\n"
            f"SOURCES_JSON: {json.dumps(sources_payload, ensure_ascii=False)}\n"
            "Write the final answer in Markdown. Cite sources with [n] markers that "
            "match SOURCES_JSON. Do not add sources that are not listed."
        )
        messages = [
            {
                "role": "system",
                "content": (
                    "[ROLE:writer] You are the writer of a research pipeline. Merge the "
                    "bounded research notes into one cited answer for the user."
                ),
            },
            {"role": "user", "content": prompt},
        ]
        try:
            answer = await self._stream_node(
                run, node_id, messages, channel="answer", max_tokens=settings.max_tokens_writer
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._node_end(run, node_id, "failed", detail=f"{type(exc).__name__}: {exc}")
            raise
        self._node_end(run, node_id, "done", detail=f"{len(answer)} chars, {len(citations)} citations")
        return answer

    # --- termination --------------------------------------------------
    def _handle_cancel(self, run: Run) -> None:
        run.status = "cancelled"
        cancelled_nodes: List[str] = []
        for node in list(run.nodes.values()):
            if node["status"] == "running":
                self._node_end(run, node["id"], "cancelled", detail="cancelled by user")
                cancelled_nodes.append(node["id"])
        requested_at = run.metrics.get("cancel_requested_at")
        latency = round((time.time() - requested_at) * 1000.0, 2) if requested_at else None
        run.metrics["cancelled_nodes"] = cancelled_nodes
        run.metrics["cancel_latency_ms"] = latency
        run.metrics["tokens_before_cancel"] = sum(
            node.get("tokens_out", 0) for node in run.nodes.values()
        )
        self._finalize(
            run,
            "run_cancelled",
            reason="user_cancel",
            cancelled_nodes=cancelled_nodes,
            cancel_latency_ms=latency,
            partial_answer=run.answer,
        )

    def _finalize(self, run: Run, event_type: str, **extra: Any) -> None:
        run.ended_at = time.time()
        nodes = list(run.nodes.values())
        run.metrics.update(
            {
                "duration_ms": run.duration_ms(),
                # +1 for the terminal event published right after this update
                "event_count": run.bus.published + 1,
                "node_count": len(nodes),
                "tokens_in": sum(node.get("tokens_in", 0) for node in nodes),
                "tokens_out": sum(node.get("tokens_out", 0) for node in nodes),
                "citation_count": len(run.citations),
                "context_tokens_after": run.context_budget.get("tokens_after"),
            }
        )
        payload: Dict[str, Any] = {
            "status": run.status,
            "answer": run.answer,
            "duration_ms": run.duration_ms(),
            "metrics": run.metrics,
        }
        payload.update(extra)
        run.bus.publish(event_type, **payload)


orchestrator = Orchestrator()
