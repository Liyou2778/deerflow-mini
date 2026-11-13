"""Context-budget policy for parent -> child agent summarisation.

Researchers can each produce unbounded text.  Before the writer sees their
output, the orchestrator applies a deterministic token budget:

* ``per_item_budget = max(min_item, total_budget // n)``
* each item longer than that is cut head+tail (strategy ``trim_head_tail``),
  keeping 70% of the prefix and 30% of the suffix, with an explicit marker so
  the trace is auditable
* if the joined text is still over budget, the last items are trimmed further.

Token counts are estimates (4 chars/token) so the MVP has no tokenizer
dependency.  The strategy name and before/after numbers are reported in the
run trace and emitted as a ``context_trim`` event.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


def estimate_tokens(text: str) -> int:
    if not text:
        return 0
    return max(1, (len(text) + 3) // 4)


@dataclass
class BudgetResult:
    texts: List[str]
    meta: Dict[str, Any] = field(default_factory=dict)


def trim_head_tail(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    keep = max(24, max_chars - 24)
    head = max(1, int(keep * 0.7))
    tail = max(1, keep - head)
    removed = len(text) - head - tail
    return f"{text[:head]}\n...[context trimmed: {removed} chars]...\n{text[-tail:]}"


def fit_results(
    results: List[Dict[str, Any]],
    budget_tokens: int,
    strategy: str = "trim_head_tail",
) -> BudgetResult:
    """Fit a list of {label, text} child results into ``budget_tokens``."""
    if not results:
        return BudgetResult(texts=[], meta={"strategy": strategy, "items": 0, "budget_tokens": budget_tokens})

    per_item_tokens = max(30, budget_tokens // len(results))
    per_item_chars = per_item_tokens * 4

    fitted: List[str] = []
    before_total = 0
    trimmed_items = 0
    for item in results:
        label = str(item.get("label") or item.get("id") or "result")
        text = str(item.get("text") or "")
        before_total += estimate_tokens(text)
        if strategy == "trim_head_tail":
            clipped = trim_head_tail(text, per_item_chars)
        elif strategy == "truncate":
            clipped = text[:per_item_chars]
        else:  # "none" keeps everything but the overall cap still applies
            clipped = text
        if clipped != text:
            trimmed_items += 1
        fitted.append(f"[{label}]\n{clipped}")

    joined = "\n\n".join(fitted)
    joined = trim_head_tail(joined, max(120, budget_tokens * 4 - 80))
    after_total = estimate_tokens(joined)

    meta = {
        "strategy": strategy,
        "items": len(results),
        "budget_tokens": budget_tokens,
        "per_item_tokens": per_item_tokens,
        "tokens_before": before_total,
        "tokens_after": after_total,
        "trimmed_items": trimmed_items,
        "tokens_saved": max(0, before_total - after_total),
        "estimated": True,
        "token_estimator": "ceil(len/4)",
    }
    return BudgetResult(texts=fitted, meta=meta)
