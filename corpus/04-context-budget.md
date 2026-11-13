# Context Budgets for Multi-Agent Systems

Source: https://example.org/context-budget
Tags: context, tokens, budget, summarisation, trimming, trace

Every model call has a hard context limit, and every extra token costs latency
and money. In a planner/researcher/writer pipeline the writer is the natural
merge point: it receives N child summaries and must fit them plus the user
question and recent conversation into one prompt.

A simple and auditable policy is a fixed token budget for child results. Split
the budget across results, trim each item with an explicit head+tail cut, then
re-check the total and trim again if needed. Mark trimmed regions inline so the
trace shows exactly what the writer saw. More advanced policies ask a cheap
model to summarise each child, deduplicate overlapping claims, or rank evidence
by relevance before packing.

Whatever the policy, report it. A trace entry should include the strategy name,
the budget, estimated tokens before and after, the number of trimmed items and
the estimator used. Without those numbers a context bug looks like a model bug.

Estimation can be approximate in an MVP: four characters per token is within
about 20 percent for English prose and avoids shipping a tokenizer. Document the
approximation, and keep the budget configurable so operators can tune it per
model.
