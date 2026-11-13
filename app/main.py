"""FastAPI app: REST + resumable SSE + static single-page UI."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .config import STATIC_DIR, settings
from .events import TERMINAL_EVENT_TYPES
from .llm import llm_info
from .orchestrator import orchestrator
from .store import Run, store
from .tools import registry

logger = logging.getLogger("deerflow_mini")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    logger.info(
        "deerflow-mini starting | provider=%s model=%s port=%s offline=%s",
        llm_info()["provider"],
        llm_info()["model"],
        settings.port,
        settings.offline,
    )
    yield
    pending = [run for run in store.runs.values() if run.task and not run.task.done()]
    for run in pending:
        run.task.cancel()
    if pending:
        await asyncio.gather(*(run.task for run in pending), return_exceptions=True)
    logger.info("deerflow-mini stopped")


app = FastAPI(title="deerflow-mini", version=settings.version, lifespan=lifespan)


class CreateSessionRequest(BaseModel):
    title: Optional[str] = None


class CreateRunRequest(BaseModel):
    session_id: str
    query: str


# --- sessions -------------------------------------------------------------

@app.post("/api/sessions")
async def create_session(payload: Optional[CreateSessionRequest] = None) -> Dict[str, Any]:
    title = (payload.title if payload and payload.title else "New session").strip() or "New session"
    session = store.create_session(title)
    return {"session": session.snapshot(store.runs)}


@app.get("/api/sessions")
async def list_sessions() -> Dict[str, Any]:
    return {"sessions": store.list_sessions()}


@app.get("/api/sessions/{session_id}")
async def get_session(session_id: str) -> Dict[str, Any]:
    session = store.get_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="session not found")
    data = session.snapshot(store.runs)
    data["history"] = session.history
    return {"session": data}


# --- runs -----------------------------------------------------------------

def _run_or_404(run_id: str) -> Run:
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    return run


@app.post("/api/runs")
async def create_run(payload: CreateRunRequest) -> Dict[str, Any]:
    session = store.get_session(payload.session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="session not found")
    query = (payload.query or "").strip()
    if not query:
        raise HTTPException(status_code=422, detail="query must not be empty")
    run = store.create_run(session, query)
    orchestrator.start(run)
    return {
        "run": run.snapshot(),
        "run_id": run.id,
        "events_url": f"/api/runs/{run.id}/events",
        "trace_url": f"/api/runs/{run.id}/trace",
    }


@app.get("/api/runs")
async def list_runs(session_id: Optional[str] = None, limit: int = 20) -> Dict[str, Any]:
    runs = sorted(store.runs.values(), key=lambda item: item.created_at, reverse=True)
    if session_id:
        runs = [run for run in runs if run.session_id == session_id]
    return {"runs": [run.snapshot() for run in runs[: max(1, min(limit, 100))]]}


@app.get("/api/runs/{run_id}")
async def get_run(run_id: str) -> Dict[str, Any]:
    return {"run": _run_or_404(run_id).snapshot()}


@app.post("/api/runs/{run_id}/cancel")
async def cancel_run(run_id: str) -> Dict[str, Any]:
    run = _run_or_404(run_id)
    if run.is_terminal():
        return {"run": run.snapshot(), "cancel_requested": False, "already_terminal": True}
    run.cancel_requested = True
    run.metrics["cancel_requested_at"] = time.time()
    task = run.task
    if task is not None and not task.done():
        task.cancel()
        requested_at = run.metrics["cancel_requested_at"]
        await asyncio.wait({task}, timeout=5.0)
        if not task.done():
            logger.warning("run %s did not stop within 5s of cancellation", run.id)
        orchestrator.force_cancel(run)
        run.metrics.setdefault(
            "cancel_observed_ms", round((time.time() - requested_at) * 1000.0, 1)
        )
    return {
        "run": run.snapshot(),
        "cancel_requested": True,
        "already_terminal": False,
        "task_cancelled": bool(task is not None and task.done() and not task.cancelled()),
    }


@app.get("/api/runs/{run_id}/trace")
async def get_trace(run_id: str) -> Dict[str, Any]:
    run = _run_or_404(run_id)
    return {
        "run_id": run.id,
        "status": run.status,
        "duration_ms": run.duration_ms(),
        "event_count": run.bus.published,
        "last_event_id": run.bus.last_seq,
        "buffer": {
            "capacity": settings.event_buffer_size,
            "buffered": len(run.bus.buffered()),
            "oldest_seq": run.bus.oldest_seq(),
        },
        "nodes": list(run.nodes.values()),
        "citations": run.citations,
        "context_budget": run.context_budget,
        "metrics": run.metrics,
        "error": run.error,
    }


# --- SSE ------------------------------------------------------------------

def _reset_frame(run: Run, since: int) -> str:
    payload = {
        "type": "stream_reset",
        "run_id": run.id,
        "requested_last_event_id": since,
        "oldest_seq": run.bus.oldest_seq(),
        "last_event_id": run.bus.last_seq,
        "reason": "history_evicted_from_ring_buffer",
    }
    return f"event: stream_reset\ndata: {json.dumps(payload)}\n\n"


@app.get("/api/runs/{run_id}/events")
async def run_events(
    run_id: str,
    request: Request,
    last_event_id: int = Query(default=0, ge=0),
) -> StreamingResponse:
    run = _run_or_404(run_id)
    header_id = request.headers.get("last-event-id")
    if header_id:
        try:
            last_event_id = max(0, int(header_id))
        except ValueError:
            pass

    queue, missed, evicted = run.bus.subscribe(last_event_id)

    async def event_stream() -> AsyncIterator[str]:
        try:
            yield f": deerflow-mini stream seq={last_event_id}\n\n"
            if evicted:
                yield _reset_frame(run, last_event_id)
            for event in missed:
                yield event.to_sse()
            if run.is_terminal():
                return
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=settings.heartbeat_seconds)
                except asyncio.TimeoutError:
                    if run.is_terminal() and queue.empty():
                        return
                    if await request.is_disconnected():
                        return
                    yield ": heartbeat\n\n"
                    continue
                yield event.to_sse()
                if event.type in TERMINAL_EVENT_TYPES:
                    while not queue.empty():
                        yield queue.get_nowait().to_sse()
                    return
        finally:
            run.bus.unsubscribe(queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --- meta -----------------------------------------------------------------

@app.get("/api/health")
async def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "app": settings.app_name,
        "version": settings.version,
        "time": time.time(),
        "llm": llm_info(),
        "offline": settings.offline,
        "researcher_count": settings.researcher_count,
        "event_buffer_size": settings.event_buffer_size,
        "tools": registry.names(),
        "sessions": len(store.sessions),
        "runs": len(store.runs),
    }


app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
