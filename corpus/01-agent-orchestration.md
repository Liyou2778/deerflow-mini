# Agent Orchestration Patterns

Source: https://example.org/agent-orchestration
Tags: agents, orchestration, planner, researcher, writer, workflow

Multi-agent orchestration splits one user request into roles that each have a
small prompt and a narrow responsibility. A common shape is planner -> parallel
researchers -> writer. The planner decomposes the question into focused
sub-questions, the researchers gather evidence concurrently, and the writer
merges the evidence into one answer.

The orchestrator owns the state machine, not the individual agents. Each node
moves through pending, running, done or failed, and every transition is emitted
as an event. Serialising node state as events gives the UI an honest timeline,
makes debugging possible after the fact, and lets a client reconnect without
replaying business logic.

Parallelism is usually expressed with asyncio.gather over researcher coroutines.
Fan-out is cheap because each researcher mostly waits on network or tool I/O.
Fan-in is where context engineering matters: the parent must merge child outputs
without exceeding its own context window, so budgets and summarisation policies
belong in the orchestrator, not in the leaf agents.

A practical rule is that child agents return evidence plus short summaries,
while the parent owns the final narrative. This keeps role prompts stable and
makes the merge step testable in isolation.
