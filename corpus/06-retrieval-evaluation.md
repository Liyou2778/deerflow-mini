# Retrieval, Citations and Evaluation

Source: https://example.org/retrieval-evaluation
Tags: retrieval, citations, evaluation, grounding, corpus, search

A research assistant is only as trustworthy as its citations. Each claim that
depends on an external source should carry a numbered marker that resolves to a
URL, and the writer should never invent a marker that no retrieved source
supports. Assign citation indices deterministically after retrieval so that
parallel workers cannot race on numbering.

Offline corpora are useful for development: a small set of markdown documents
with source URLs, tags and body text is enough to exercise search, fetching and
citation plumbing without network access. A local search tool can rank documents
by term overlap with title and tag weighting, then return snippets. A fetch tool
maps a URL back to the cached document, which makes the offline mode realistic
instead of fake.

Evaluation for such a pipeline is mostly observability. Measure how many events
a run emits, how long each node takes, how many tokens were passed to the writer
after budgeting, and whether the terminal event always arrives. Run the same
deterministic prompt twice and compare event counts and node ordering. These
numbers catch regressions in orchestration far earlier than subjective review of
the final answer.
