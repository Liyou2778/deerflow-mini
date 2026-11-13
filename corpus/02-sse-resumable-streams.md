# Server-Sent Events and Resumable Streams

Source: https://example.org/sse-resumable-streams
Tags: sse, streaming, eventsource, reconnect, last-event-id, http

Server-Sent Events keep one HTTP response open and push text frames to the
browser. Each frame is a small block of fields: `id`, `event` and `data`, ended
by a blank line. The transport is plain HTTP, so proxies and browser dev tools
can inspect it, and the client is the built-in EventSource object rather than a
WebSocket implementation.

The feature that makes SSE suitable for long agent runs is resumption. When a
connection drops, the browser reconnects automatically and sends a
`Last-Event-ID` header containing the last id it saw. A server that keeps a
bounded buffer of recent events can replay everything after that id before it
resumes live delivery. The client state machine is then simple: append events
with a larger sequence number, ignore duplicates, and reset the view if the
server says history was evicted.

Two details are easy to get wrong. First, replay and live subscription must be
atomic, otherwise events published between the two steps are lost; publishing
synchronously on the event loop is enough in a single-process asyncio server.
Second, terminal events must be flushed before the response closes, and the
server should send periodic comment heartbeats so idle connections are not
silently dropped by intermediaries.

A sequence number per run, a bounded ring buffer and an explicit reset event are
therefore the minimum viable design for an agent event stream.
