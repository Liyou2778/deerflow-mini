# DeerFlow and Deep Research Workflows

Source: https://example.org/deerflow-overview
Tags: deerflow, deepresearch, workflow, report, tools, citations

DeerFlow is a research workflow that combines a planner, parallel researchers,
tool calls and a final report writer. The planner turns a broad question into
sub-tasks, researchers use retrieval tools to gather sources, and the writer
assembles a structured answer with inline citations. A trace view exposes every
step so the user can audit where claims came from.

The valuable ideas are architectural rather than code-level. Roles are small and
single-purpose. Tools are registered centrally with explicit permissions, so an
agent can only call tools in its namespace. Research results are intermediate
artifacts that the writer composes from, not raw chat history. Every node
transition is observable.

An MVP is a few hundred lines: an event bus with sequence numbers, an asyncio
orchestrator, a mockable LLM interface, a local corpus for offline search, and a
static single-page UI. The mock LLM is important: it makes the whole system
deterministic, testable and runnable without API keys, while the same
orchestration code can be switched to a hosted model through an
OpenAI-compatible endpoint.

Known gaps in a small reproduction include durable storage, multi-user auth,
streaming tool output and evaluation harnesses. Those are worth naming in the
README rather than pretending the system is production ready.
