# Cancellation and Task Propagation in asyncio

Source: https://example.org/asyncio-cancellation
Tags: cancellation, asyncio, tasks, timeout, cleanup, streaming

asyncio cancellation is cooperative: `task.cancel()` schedules a CancelledError
to be raised at the next await point inside the task. Code that never awaits, or
that catches CancelledError and keeps running, defeats cancellation. A correct
implementation lets the exception propagate after running synchronous cleanup,
and only suppresses it at the outermost boundary where the cancelled status is
recorded.

Structured concurrency helps. `asyncio.gather` propagates cancellation to its
children, so cancelling the orchestrator task also cancels every researcher
coroutine it is waiting on. Inline tool calls inherit the same task, so their
awaits are interrupted too. If a tool owns a network response, wrap it in
`async with` so the response is closed when the cancellation unwinds.

For LLM streaming, cancellation must reach the HTTP stream, not just the
consumer loop. With httpx, iterating `response.aiter_lines()` inside
`async with client.stream(...)` is enough: the context manager closes the
connection during unwinding and the provider stops generating tokens. A run
trace should record the cancelled node, how many tokens had been produced and
how long cancellation took from request to terminal event.

The UI contract is a single terminal `run_cancelled` event, published after the
node cleanup events, so a client never waits forever for a run that was stopped.
