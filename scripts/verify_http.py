"""End-to-end HTTP verification for deerflow-mini (run against a live server).

    ..\\.venv\\Scripts\\python.exe scripts\\verify_http.py

Checks the three non-negotiable behaviours:
  1. full run: per-run seq is contiguous 1..N and the stream ends with run_end
  2. disconnection resumption: drop the HTTP stream, reconnect with the
     standard ``Last-Event-ID`` header, assert the missing events are replayed
     from seq+1 with no hole and the stream still terminates
  3. cancellation: POST /cancel while the writer node is streaming, assert the
     run task really stops, the stream ends with run_cancelled, the trace marks
     the writer cancelled and reports cancellation latency

Prints a metrics block used for the README.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

BASE = "http://127.0.0.1:8201"
QUERY = "How do multi-agent research pipelines handle context budgets and resumable streams?"


class SseReader:
    """Minimal SSE parser used by the checks below."""

    def __init__(self) -> None:
        self.events: List[Dict[str, Any]] = []
        self._current: Dict[str, Any] = {}

    def feed(self, line: str) -> Optional[Dict[str, Any]]:
        if line.startswith("id: "):
            self._current["id"] = int(line[4:])
        elif line.startswith("event: "):
            self._current["event"] = line[7:]
        elif line.startswith("data: "):
            self._current["data"] = json.loads(line[6:])
        elif line == "" and self._current:
            event = self._current
            self._current = {}
            self.events.append(event)
            return event
        return None

    @property
    def last_seq(self) -> int:
        return int(self.events[-1]["id"]) if self.events else 0


async def read_stream(
    client: httpx.AsyncClient,
    run_id: str,
    *,
    last_event_id: int = 0,
    stop_after: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """Read an SSE stream. Returns (events, last_seq) and closes when finished."""
    reader = SseReader()
    headers = {"Last-Event-ID": str(last_event_id)} if last_event_id else {}
    params = {} if last_event_id else {"last_event_id": 0}
    async with client.stream(
        "GET", f"{BASE}/api/runs/{run_id}/events", params=params, headers=headers, timeout=30.0
    ) as response:
        response.raise_for_status()
        assert response.headers["content-type"].startswith("text/event-stream")
        async for line in response.aiter_lines():
            event = reader.feed(line)
            if event is not None and stop_after is not None and len(reader.events) >= stop_after:
                break
    return reader.events, reader.last_seq


def assert_contiguous(events: List[Dict[str, Any]], start: int = 1) -> None:
    seqs = [event["id"] for event in events]
    expected = list(range(start, start + len(seqs)))
    assert seqs == expected, f"seq hole: first={seqs[:3]} last={seqs[-3:]} expected_start={start}"


async def create_session(client: httpx.AsyncClient) -> str:
    response = await client.post(f"{BASE}/api/sessions", json={})
    response.raise_for_status()
    return response.json()["session"]["id"]


async def start_run(client: httpx.AsyncClient, session_id: str, query: str = QUERY) -> str:
    response = await client.post(f"{BASE}/api/runs", json={"session_id": session_id, "query": query})
    response.raise_for_status()
    return response.json()["run_id"]


async def wait_for_status(client: httpx.AsyncClient, run_id: str, statuses: set[str], timeout: float = 10.0) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = await client.get(f"{BASE}/api/runs/{run_id}")
        run = response.json()["run"]
        if run["status"] in statuses:
            return run
        await asyncio.sleep(0.05)
    raise AssertionError(f"run {run_id} did not reach {statuses}")


def print_node_table(trace: Dict[str, Any]) -> None:
    for node in trace["nodes"]:
        print(
            f"  {node['id']:<14} {node['status']:<9} "
            f"{node['duration_ms']:>8.1f} ms  in={node['tokens_in']:>3} out={node['tokens_out']:>3}"
        )


async def main() -> int:
    failures: List[str] = []

    async with httpx.AsyncClient() as client:
        health = (await client.get(f"{BASE}/api/health")).json()
        print(f"health              : {health['status']} provider={health['llm']['provider']} model={health['llm']['model']}")
        print(f"offline             : {health['offline']}  tools={health['tools']}  buffer={health['event_buffer_size']}")

        session_id = await create_session(client)
        print(f"session             : {session_id}")

        # ---------- 1. full run -------------------------------------
        run_id = await start_run(client, session_id)
        print(f"run 1               : {run_id}")
        events, _ = await read_stream(client, run_id)
        terminal = events[-1]
        print(f"events              : {len(events)}  first={events[0]['id']} last={terminal['id']}")
        assert_contiguous(events)
        assert terminal["event"] in ("run_end", "run_cancelled"), terminal
        assert terminal["data"]["status"] == "done"
        trace = (await client.get(f"{BASE}/api/runs/{run_id}/trace")).json()
        assert trace["event_count"] == len(events), (trace["event_count"], len(events))
        assert trace["last_event_id"] == events[-1]["id"]
        print(f"run_end status      : {terminal['data']['status']}")
        print(f"end-to-end duration : {trace['duration_ms']} ms (server measured)")
        print_node_table(trace)
        print(f"citations           : {len(trace['citations'])}")
        print(f"context budget      : {trace['context_budget']}")
        print(f"replay buffer       : {trace['buffer']}")

        # ---------- 2. disconnect + Last-Event-ID resume ------------
        session2 = await create_session(client)
        run2 = await start_run(client, session2, "Resumable stream disconnect test")
        first_events, last_seq = await read_stream(client, run2, stop_after=6)
        print(f"\nrun 2 (resume)      : {run2}")
        print(f"disconnected after  : {len(first_events)} events, last_event_id={last_seq}")
        assert len(first_events) == 6
        restored, restored_last = await read_stream(client, run2, last_event_id=last_seq)
        print(f"reconnected header  : Last-Event-ID={last_seq}")
        print(f"replayed events     : {len(restored)} first_seq={restored[0]['id']} last_seq={restored_last}")
        assert restored, "no events replayed after reconnect"
        assert restored[0]["id"] == last_seq + 1, (restored[0]["id"], last_seq)
        assert_contiguous(restored, start=last_seq + 1)
        full = first_events + restored
        assert_contiguous(full)
        assert full[-1]["event"] in ("run_end", "run_cancelled")
        print(f"resume result       : contiguous 1..{restored_last}, terminal={full[-1]['event']}")

        # ---------- 3. cancel mid-writer ----------------------------
        session3 = await create_session(client)
        run3 = await start_run(client, session3, "Cancel while the writer is streaming")
        print(f"\nrun 3 (cancel)      : {run3}")
        snapshot = await wait_for_status(client, run3, {"running"})
        while snapshot["status"] == "running":
            writer = next((node for node in snapshot["nodes"] if node["id"] == "writer"), None)
            if writer and writer["status"] == "running":
                break
            await asyncio.sleep(0.03)
            snapshot = (await client.get(f"{BASE}/api/runs/{run3}")).json()["run"]
            if snapshot["status"] != "running":
                raise AssertionError("run finished before cancellation could be tested")
        print("writer node state   : running (streaming answer tokens)")
        cancel_started = time.monotonic()
        cancel_response = await client.post(f"{BASE}/api/runs/{run3}/cancel")
        cancel_response.raise_for_status()
        cancel_wall_ms = round((time.monotonic() - cancel_started) * 1000.0, 1)
        cancel_body = cancel_response.json()
        print(f"cancel HTTP status  : {cancel_response.status_code} in {cancel_wall_ms} ms")
        events3, _ = await read_stream(client, run3)
        assert_contiguous(events3)
        terminal3 = events3[-1]
        print(f"cancel last event   : {terminal3['event']} status={terminal3['data'].get('status')}")
        print(f"cancelled nodes     : {terminal3['data'].get('cancelled_nodes')}")
        print(f"cancel latency      : {terminal3['data'].get('cancel_latency_ms')} ms (event -> task stopped)")
        trace3 = (await client.get(f"{BASE}/api/runs/{run3}/trace")).json()
        print(f"run status          : {trace3['status']}  cancelled={trace3['metrics'].get('cancelled_nodes')}")
        print(f"partial answer kept : {len(trace3['metrics'].get('cancelled_nodes') or [])} cancelled node(s)")
        print(f"events until cancel : {len(events3)}")
        assert terminal3["event"] == "run_cancelled", terminal3["event"]
        assert trace3["status"] == "cancelled", trace3["status"]
        assert trace3["metrics"].get("cancelled_nodes"), "no node was marked cancelled"
        assert cancel_body["run"]["status"] == "cancelled"
        assert trace3["metrics"].get("cancel_latency_ms") is not None

        print("\nALL HTTP CHECKS PASSED")
    return 0 if not failures else 1


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except AssertionError as exc:
        print(f"\nCHECK FAILED: {exc}")
        raise SystemExit(1)
