"""In-memory session / run store.

MVP scope: state lives in the process.  A real deployment would swap this for
SQLite/Postgres plus a durable event log; the API shape would not change.
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional

from .config import settings
from .events import EventBus


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class Session:
    def __init__(self, title: str = "New session") -> None:
        self.id = new_id("sess")
        self.title = title.strip() or "New session"
        self.created_at = time.time()
        self.run_ids: List[str] = []
        self.history: List[Dict[str, str]] = []  # [{"role": ..., "content": ...}]

    def snapshot(self, runs: Dict[str, "Run"]) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "created_at": self.created_at,
            "run_count": len(self.run_ids),
            "runs": [runs[rid].snapshot(include_events=False) for rid in self.run_ids if rid in runs],
        }


class Run:
    def __init__(self, session_id: str, query: str) -> None:
        self.id = new_id("run")
        self.session_id = session_id
        self.query = query
        self.status = "pending"  # pending | running | done | cancelled | failed
        self.created_at = time.time()
        self.started_at: Optional[float] = None
        self.ended_at: Optional[float] = None
        self.answer = ""
        self.error: Optional[str] = None
        self.cancel_requested = False
        self.task: Optional[Any] = None  # asyncio.Task, set by the orchestrator
        self.bus = EventBus(self.id, capacity=settings.event_buffer_size)
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.citations: List[Dict[str, Any]] = []
        self.context_budget: Dict[str, Any] = {}
        self.metrics: Dict[str, Any] = {}

    def is_terminal(self) -> bool:
        return self.status in ("done", "cancelled", "failed")

    def duration_ms(self) -> Optional[float]:
        end = self.ended_at if self.ended_at is not None else None
        start = self.started_at
        if start is None or end is None:
            return None
        return round((end - start) * 1000.0, 1)

    def snapshot(self, include_events: bool = False) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "id": self.id,
            "session_id": self.session_id,
            "query": self.query,
            "status": self.status,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration_ms": self.duration_ms(),
            "answer": self.answer,
            "error": self.error,
            "cancelled": self.status == "cancelled",
            "event_count": self.bus.published,
            "last_event_id": self.bus.last_seq,
            "citation_count": len(self.citations),
            "citations": self.citations,
            "nodes": list(self.nodes.values()),
            "context_budget": self.context_budget,
            "metrics": self.metrics,
        }
        if include_events:
            data["events"] = [event.payload() for event in self.bus.buffered()]
        return data


class Store:
    def __init__(self) -> None:
        self.sessions: Dict[str, Session] = {}
        self.runs: Dict[str, Run] = {}

    # --- sessions -----------------------------------------------------
    def create_session(self, title: str = "New session") -> Session:
        session = Session(title)
        self.sessions[session.id] = session
        return session

    def get_session(self, session_id: str) -> Optional[Session]:
        return self.sessions.get(session_id)

    def list_sessions(self) -> List[Dict[str, Any]]:
        sessions = sorted(self.sessions.values(), key=lambda s: s.created_at, reverse=True)
        return [session.snapshot(self.runs) for session in sessions]

    # --- runs ---------------------------------------------------------
    def create_run(self, session: Session, query: str) -> Run:
        run = Run(session.id, query)
        self.runs[run.id] = run
        session.run_ids.append(run.id)
        return run

    def get_run(self, run_id: str) -> Optional[Run]:
        return self.runs.get(run_id)


store = Store()
