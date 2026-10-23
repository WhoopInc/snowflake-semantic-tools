# Model comparison report: `compare.json`, schema `sst.compare/1`

`sst eval --compare-models` writes one `compare.json` per run, beside `report.html`,
and prints the same object with `--output json`. Every key below is always present; a value
that is not known is `null` unless the key says otherwise. Numbers are JSON numbers, never
strings. Times are ISO 8601 with offset. Lists are in display order.

```json
{
  "schema": "sst.compare/1",
  "generated_at": "2026-10-22T14:03:11-04:00",
  "cost_read_at": "2026-10-22T15:10:02-04:00",
  "target": "dev",
  "run_id": "c7f2a19e",
  "git_sha": "703f69a",
  "evals": [
    {
      "eval": "jaffle_analyst",
      "kind": "agent",
      "subject": {
        "name": "jaffle_analyst",
        "qualified_name": "JAFFLE_SALES_DEV.AGENTS.JAFFLE_ANALYST",
        "released_version": "VERSION$4",
        "pinned_model": "claude-sonnet-4-6"
      },
      "dataset": {"name": "JAFFLE_ANALYST_EVAL", "version": "V3", "questions": 8},
      "runs_per_model": 2,
      "metrics": [
        {"name": "answer_correctness", "gated": true, "threshold": 0.9},
        {"name": "logical_consistency", "gated": false, "threshold": null}
      ],
      "models": [
        {
          "model": "claude-sonnet-4-6",
          "role": "pinned",
          "version": "VERSION$4",
          "alias": null,
          "lifecycle": "ga",
          "status": "completed",
          "error": null,
          "rank": 2,
          "run_names": ["JAFFLE_ANALYST_CMP_C7F2A19E_CLAUDE_SONNET_4_6_1", "..._2"],
          "metrics": {
            "answer_correctness": {"mean": 0.875, "min": 0.85, "max": 0.9, "passed": 7, "total": 8},
            "logical_consistency": {"mean": 0.95, "min": 0.94, "max": 0.96, "passed": 8, "total": 8}
          },
          "latency_ms": {"mean": 8120, "p95": 14210},
          "tokens": {"agent_input": 182340, "agent_output": 9120, "llm_calls": 41, "judge": 52010},
          "cost": {
            "agent_credits": {"state": "known", "value": 0.4123},
            "sql_credits": {"state": "pending", "value": null},
            "judge_tokens": {"state": "known", "value": 52010},
            "credits_per_question": {"state": "known", "value": 0.0258}
          },
          "request_ids": ["0cea899c-...", "..."]
        }
      ],
      "questions": [
        {
          "id": "q1",
          "text": "What was revenue by month in 2025?",
          "results": {
            "claude-sonnet-4-6": {"passed": true, "score": 1.0, "reason": "...", "route": null},
            "claude-opus-4-8": {"passed": false, "score": 0.5, "reason": "...", "route": null}
          },
          "changes": {"claude-opus-4-8": "breaks"}
        }
      ],
      "verdict": {
        "kind": "winner",
        "model": "claude-opus-4-8",
        "summary": "claude-opus-4-8 beats the pinned claude-sonnet-4-6 on answer_correctness (0.94 vs 0.88) outside the spread, at 1.8x the credits per question.",
        "pin_suggestion": "# agent.yml of agent 'jaffle_analyst'\nconfig:\n  orchestration_model: claude-opus-4-8",
        "excluded": [{"model": "claude-haiku-4-5-preview", "reason": "preview"}]
      },
      "notes": [
        "Held fixed: dataset JAFFLE_ANALYST_EVAL V3, metric versions, judge claude-sonnet-4-6, everything in the specification but models.orchestration.",
        "Usage views lag up to an hour; values marked pending fill in with --refresh-cost."
      ]
    }
  ]
}
```

## Enumerations

- `kind`: `agent` or `cowork_profile`.
- `models[].role`: `pinned` (the released version, the model the project pins) or `candidate`.
  Exactly one `pinned` entry per eval, first in the list; candidates follow in rank order.
- `models[].lifecycle`: `ga`, `preview`, `internal`, `deprecated`, `unknown`.
- `models[].status`: `completed`, `partial` (some runs failed), `failed` (no run scored).
- `cost.*.state`: `known`, `pending` (usage not landed yet), `unavailable` (the role may not
  read the usage view, or nothing can attribute it). `value` is null unless `known`.
- `questions[].results[<model>].route`: for a CoWork profile, the subagents the router chose
  (fully qualified names, `[]` for a decline); null for an agent.
- `questions[].changes[<model>]`: against the pinned model, `fixes` (pinned failed, candidate
  passed), `breaks` (pinned passed, candidate failed), or absent when unchanged. The pinned model
  never appears in `changes`.
- `verdict.kind`: `winner` (a candidate beats the pinned model outside the spread, each of
  the two having completed at least 2 runs), `pinned_holds` (no candidate beats it),
  `no_clear_winner` (the leader's range overlaps the pinned model's, or either completed fewer
  than 2 runs, so a lead cannot be told from run-to-run noise),
  `incomplete` (the pinned model or every candidate failed).
- `verdict.pin_suggestion`: when `kind` is `winner`, the YAML that pins the winner for this
  subject alone: a comment naming the subject's file, then its `config:` block, lines joined
  by `\n`; else null.

## Scores

`metrics[<name>].mean` is the mean over every scored question of every run; `min`/`max` are the
lowest and highest per-run means (the spread); `passed`/`total` count questions passing the
metric's threshold (or scoring 1.0 when it has none) in the run nearest the mean.
`questions[].results[<model>]` uses the first gated metric (else the first metric), from the
same run.
