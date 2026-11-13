"""In-process smoke test + metrics capture for deerflow-mini.

Usage (from the project root):
    ..\\.venv\\Scripts\\python.exe scripts\\inproc_check.py

It exercises the orchestrator directly (no HTTP) to verify:
  * seq continuity and terminal events
  * node state machine + durations
  * context budget metadata
  * cancellation really aborts the run task and the LLM stream
  * replay-from-seq returns the missing tail
Prints the numbers used as a cross-check for the README.
"""

from __future__ import annotations

import asyncio
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.context import fit_results  # noqa: E402
from app.orchestrator import Orchestrator  # noqa: E402
from app.store import Store  # noqa: E402
from app.tools import ToolPermissionError, registry  # noqa: E402

QUERY = "How do multi-agent research pipelines handle context budgets and resumable streams?"


async def main() -> None:
    store = Store()
    orchestrator = Orchestrator(store)
    session = store.create_session()
    session.title = "smoke"

    run = store.create_run(session, QUERY)
    await orchestrator.start(run)

    events = run.bus.buffered()
    seqs = [event.seq for event in events]
    print(f"run status          : {run.status}")
    print(f"events published    : {run.bus.published} (buffered {len(events)})")
    print(f"seq continuous      : {seqs == list(range(1, len(seqs) + 1))} first={seqs[0]} last={seqs[-1]}")
    print(f"last event type     : {events[-1].type}")
    print(f"duration (ms)       : {run.duration_ms()}")
    print(f"event types         : {dict(Counter(event.type for event in events))}")
    for node in run.nodes.values():
        print(
            f"  node {node['id']:<14} {node['status']:<6} "
            f"{node['duration_ms']:>7}ms in={node['tokens_in']:>3} out={node['tokens_out']:>3} "
            f"detail={node['detail']}"
        )
    print(f"citations           : {len(run.citations)}")
    print(f"context budget      : {run.context_budget}")
    print(f"answer chars        : {len(run.answer)}")
    print(f"metrics             : {run.metrics}")

    # replay contract
    queue, missed, evicted = run.bus.subscribe(5)
    run.bus.unsubscribe(queue)
    print(f"replay after seq 5  : {len(missed)} events, first seq={missed[0].seq}, evicted={evicted}")

    # --- cancellation run --------------------------------------------
    run2 = store.create_run(session, "cancel me mid writer")
    task = orchestrator.start(run2)
    for _ in range(200):
        await asyncio.sleep(0.02)
        if run2.nodes.get("writer", {}).get("status") == "running":
            break
    print(
        f"\ncancel at events    : {run2.bus.published}, "
        f"writer_running={run2.nodes.get('writer', {}).get('status')}, "
        f"answer_so_far={len(run2.answer)} chars"
    )
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("outer await raised CancelledError")
    print(f"cancel status       : {run2.status}")
    print(f"cancel last event   : {run2.bus.buffered()[-1].type}")
    print(f"cancel latency (ms) : {run2.metrics.get('cancel_latency_ms')}")
    print(f"cancelled nodes     : {run2.metrics.get('cancelled_nodes')}")
    print(f"tokens before cancel: {run2.metrics.get('tokens_before_cancel')}")
    print(f"writer tokens kept  : {run2.nodes.get('writer', {}).get('tokens_out')}")
    print(f"partial answer kept : {len(run2.answer)} chars")

    # --- permission isolation ----------------------------------------
    try:
        await registry.invoke("writer", [], "web_search", {"query": "x"})
        print("\npermission isolation: FAILED (call was allowed)")
    except ToolPermissionError as exc:
        print(f"\npermission isolation: blocked -> {exc}")

    budget = fit_results([{"label": "a", "text": "word " * 300}], 80)
    print(f"budget estimator    : {budget.meta}")


if __name__ == "__main__":
    asyncio.run(main())
