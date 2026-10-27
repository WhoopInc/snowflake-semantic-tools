"""Model comparison reports in the `sst.compare/1` schema, for the HTML report and anything else that reads one.

`sample_compare_report` is one run comparing two evals. The agent `jaffle_analyst` compares its
pinned `claude-sonnet-4-6` with `claude-opus-4-8`, the winner, and `claude-haiku-4-5-preview`, a
preview model left out of the verdict whose runs were partial: one question has no result, its
agent credits and credits per question are pending and its SQL credits unavailable. Opus fixes
q4 and q7 and breaks q6; haiku breaks q2 and q5. The CoWork profile `finance_desk` compares its
pinned model with one candidate, and the pinned model holds; each result carries the route its
router chose, `[]` for the declined q4. Every call returns a new dict the caller may mutate.
"""

from __future__ import annotations

from typing import Any

SONNET = "claude-sonnet-4-6"
OPUS = "claude-opus-4-8"
HAIKU = "claude-haiku-4-5-preview"

_QUESTIONS = (
    ("q1", "What was revenue by month in 2025?"),
    ("q2", "Which store had the highest average order value last quarter?"),
    ("q3", "How many customers ordered more than three times in 2025?"),
    ("q4", "What share of orders used a coupon, by payment method?"),
    ("q5", "List the five best-selling products by units in March."),
    ("q6", "How did repeat-customer revenue change year over year?"),
    ("q7", "What is the median days between a customer's first and second order?"),
    ("q8", "Which supply cost the most per order in Q4?"),
)
# Per model, the questions it fails on the headline metric; None marks a question with no result.
_FAILS = {SONNET: {"q4", "q7"}, OPUS: {"q6"}, HAIKU: {"q2", "q4", "q5", "q7"}}
_MISSING = {HAIKU: {"q8"}}


def known(value: float) -> dict[str, Any]:
    """A cost figure read from the usage views."""
    return {"state": "known", "value": value}


def pending() -> dict[str, Any]:
    """A cost figure whose usage has not landed yet."""
    return {"state": "pending", "value": None}


def unavailable() -> dict[str, Any]:
    """A cost figure the role cannot read, or that nothing attributes."""
    return {"state": "unavailable", "value": None}


def model_entry(model: str, *, role: str = "candidate", rank: int | None = None, **overrides: Any) -> dict[str, Any]:
    """One `models[]` entry for `model`, completed and fully costed unless `overrides` say otherwise."""
    entry: dict[str, Any] = {
        "model": model,
        "role": role,
        "version": "VERSION$4" if role == "pinned" else None,
        "alias": None if role == "pinned" else f"EVAL_{model.upper().replace('-', '_')}",
        "lifecycle": "ga",
        "status": "completed",
        "error": None,
        "rank": rank,
        "run_names": [f"CMP_C7F2A19E_{model.upper().replace('-', '_')}_{run}" for run in (1, 2)],
        "metrics": {},
        "latency_ms": {"mean": 8120, "p95": 14210},
        "tokens": {"agent_input": 182340, "agent_output": 9120, "llm_calls": 41, "judge": 52010},
        "cost": {
            "agent_credits": known(0.4123),
            "sql_credits": known(0.0188),
            "judge_tokens": known(52010),
            "credits_per_question": known(0.0258),
        },
        "request_ids": [f"0cea899c-{model}-{run}" for run in (1, 2)],
    }
    entry.update(overrides)
    return entry


def scores(mean: float, low: float, high: float, passed: int, total: int) -> dict[str, Any]:
    """One `models[].metrics[<name>]` value."""
    return {"mean": mean, "min": low, "max": high, "passed": passed, "total": total}


def sample_compare_report() -> dict[str, Any]:
    """A complete `sst.compare/1` report: the agent eval, then the CoWork profile eval."""
    return {
        "schema": "sst.compare/1",
        "generated_at": "2026-10-22T14:03:11-04:00",
        "cost_read_at": "2026-10-22T15:10:02-04:00",
        "target": "dev",
        "run_id": "c7f2a19e",
        "git_sha": "703f69a",
        "evals": [agent_eval(), profile_eval()],
    }


def agent_eval() -> dict[str, Any]:
    """The `jaffle_analyst` eval: three models, a winner, pending and unavailable costs, changes."""
    models = [
        model_entry(
            SONNET,
            role="pinned",
            rank=2,
            metrics={
                "answer_correctness": scores(0.875, 0.85, 0.9, 6, 8),
                "logical_consistency": scores(0.95, 0.94, 0.96, 8, 8),
            },
            cost={**model_entry(SONNET)["cost"], "sql_credits": pending()},
        ),
        model_entry(
            OPUS,
            rank=1,
            metrics={
                "answer_correctness": scores(0.9375, 0.925, 0.95, 7, 8),
                "logical_consistency": scores(0.97, 0.96, 0.98, 8, 8),
            },
            latency_ms={"mean": 11890, "p95": 21400},
            tokens={"agent_input": 201220, "agent_output": 14410, "llm_calls": 46, "judge": 55870},
            cost={
                "agent_credits": known(0.742),
                "sql_credits": known(0.031),
                "judge_tokens": known(55870),
                "credits_per_question": known(0.0464),
            },
        ),
        model_entry(
            HAIKU,
            rank=3,
            lifecycle="preview",
            status="partial",
            error="Run 2 stopped after 600 s waiting on Cortex Agents; 7 of 8 questions scored.",
            metrics={
                "answer_correctness": scores(0.5714, 0.5, 0.64, 3, 7),
                "logical_consistency": scores(0.81, 0.78, 0.84, 6, 7),
            },
            latency_ms={"mean": 3420, "p95": 6150},
            tokens={"agent_input": 160300, "agent_output": None, "llm_calls": 38, "judge": 44120},
            cost={
                "agent_credits": pending(),
                "sql_credits": unavailable(),
                "judge_tokens": known(44120),
                "credits_per_question": pending(),
            },
        ),
    ]
    return {
        "eval": "jaffle_analyst",
        "kind": "agent",
        "subject": {
            "name": "jaffle_analyst",
            "qualified_name": "JAFFLE_SALES_DEV.AGENTS.JAFFLE_ANALYST",
            "released_version": "VERSION$4",
            "pinned_model": SONNET,
        },
        "dataset": {"name": "JAFFLE_ANALYST_EVAL", "version": "V3", "questions": len(_QUESTIONS)},
        "runs_per_model": 2,
        "metrics": [
            {"name": "logical_consistency", "gated": False, "threshold": None},
            {"name": "answer_correctness", "gated": True, "threshold": 0.9},
        ],
        "models": models,
        "questions": [_question(qid, text, (SONNET, OPUS, HAIKU)) for qid, text in _QUESTIONS],
        "verdict": {
            "kind": "winner",
            "model": OPUS,
            "summary": f"{OPUS} beats the pinned {SONNET} on answer_correctness (0.94 vs 0.88) outside the "
            "spread, at 1.8x the credits per question.",
            "pin_suggestion": f"# agent.yml of agent 'jaffle_analyst'\nconfig:\n  orchestration_model: {OPUS}",
            "excluded": [{"model": HAIKU, "reason": "preview"}],
        },
        "notes": [
            "Held fixed: dataset JAFFLE_ANALYST_EVAL V3, metric versions, judge claude-sonnet-4-6, everything in "
            "the specification but models.orchestration.",
            "Usage views lag up to an hour; values marked pending fill in with --refresh-cost.",
        ],
    }


def profile_eval() -> dict[str, Any]:
    """The `finance_desk` CoWork profile eval: two models, the pinned one holds, routes per result."""
    revenue, margin = "JAFFLE_SALES_DEV.AGENTS.REVENUE_ANALYST", "JAFFLE_SALES_DEV.AGENTS.MARGIN_ANALYST"
    routes = {"p1": [revenue], "p2": [revenue, margin], "p3": [margin], "p4": []}
    questions = []
    for qid, prompt, fails in (
        ("p1", "What was net revenue last week?", set()),
        ("p2", "Compare gross margin and revenue growth by region.", {OPUS}),
        ("p3", "Which product line has the thinnest margin?", set()),
        ("p4", "Can you book a meeting with the CFO?", {SONNET, OPUS}),
    ):
        results = {
            model: {
                "passed": model not in fails,
                "score": 0.0 if model in fails else 1.0,
                "reason": "Declined: scheduling is out of scope." if qid == "p4" else "Matches the reference answer.",
                "route": routes[qid],
            }
            for model in (SONNET, OPUS)
        }
        questions.append({"id": qid, "text": prompt, "results": results, "changes": _changes(results, (OPUS,))})
    return {
        "eval": "finance_desk",
        "kind": "cowork_profile",
        "subject": {
            "name": "finance_desk",
            "qualified_name": "JAFFLE_SALES_DEV.PROFILES.FINANCE_DESK",
            "released_version": "VERSION$2",
            "pinned_model": SONNET,
        },
        "dataset": {"name": "FINANCE_DESK_EVAL", "version": "V1", "questions": 4},
        "runs_per_model": 2,
        "metrics": [{"name": "routing_accuracy", "gated": True, "threshold": 0.75}],
        "models": [
            model_entry(
                SONNET,
                role="pinned",
                rank=1,
                version="VERSION$2",
                metrics={"routing_accuracy": scores(0.75, 0.75, 0.75, 3, 4)},
                cost={
                    **model_entry(SONNET)["cost"],
                    "agent_credits": known(0.0488),
                    "credits_per_question": known(0.0061),
                },
            ),
            model_entry(
                OPUS,
                rank=2,
                metrics={"routing_accuracy": scores(0.5, 0.5, 0.5, 2, 4)},
                cost={
                    **model_entry(OPUS)["cost"],
                    "agent_credits": known(0.0912),
                    "credits_per_question": known(0.0114),
                },
            ),
        ],
        "questions": questions,
        "verdict": {
            "kind": "pinned_holds",
            "model": None,
            "summary": f"No candidate beats the pinned {SONNET} on routing_accuracy.",
            "pin_suggestion": None,
            "excluded": [],
        },
        "notes": ["Costs cover the router only; each subagent's own usage is billed to that agent."],
    }


def _question(qid: str, prompt: str, models: tuple[str, ...]) -> dict[str, Any]:
    results = {
        model: {
            "passed": qid not in _FAILS[model],
            "score": 0.5 if qid in _FAILS[model] else 1.0,
            "reason": "Wrong grouping: by order date, not ship date." if qid in _FAILS[model] else "Matches.",
            "route": None,
        }
        for model in models
        if qid not in _MISSING.get(model, set())
    }
    return {"id": qid, "text": prompt, "results": results, "changes": _changes(results, models[1:])}


def _changes(results: dict[str, Any], candidates: tuple[str, ...]) -> dict[str, str]:
    """`fixes` or `breaks` for each candidate whose pass differs from the pinned model's, the first key."""
    pinned = next(iter(results.values()))["passed"]
    changes = {}
    for model in candidates:
        if model in results and results[model]["passed"] != pinned:
            changes[model] = "fixes" if results[model]["passed"] else "breaks"
    return changes
