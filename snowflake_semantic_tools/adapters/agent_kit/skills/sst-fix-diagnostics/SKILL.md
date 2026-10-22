---
name: sst-fix-diagnostics
description: |-
  Use when an `sst` command reports diagnostics (codes such as SST-VAL101 or SST-REF001), or
  exits non-zero, and you need to understand and fix them: reading `-o agent` and the JSON envelope,
  `sst explain`, severities, strict mode, cascades, and when the baseline file is and is not
  the answer. Do NOT use for environment problems such as dbt parse failures, profiles, or
  connections (use sst-troubleshoot), or to decide whether SST itself is wrong (use
  sst-report-issue).
---
# Fixing what SST reports

Every problem SST reports is a diagnostic with a stable code, such as `SST-VAL405`:

- an **error** blocks the command;
- a **warning** does not, unless `--strict` or `validation.strict: true` promotes it, which
  prints `(promoted from warning)`;
- an **info** never blocks, and is counted rather than shown unless `--show-info`.

## Read the diagnostics

```bash
sst -o agent validate --no-snowflake-syntax-check
```

prints a short text document, the format `sst` picks on its own when a coding agent runs it with
stdout piped:

```text
sst validate: FAIL exit=1 errors=1 warnings=0 baselined=8 info_hidden=166 time=0.6s
E SST-REF002 semantic_models/metrics/customer_count.yml:5:46 metric:customer_count | 'customer_uuid' is not a column on customers
  fix: correct the column name, or add it to the model's columns: block
  cascade SST-MEM107 semantic_models/metrics/customer_count.yml:3:3 | metric:customer_count skipped: 1 unresolved references
next: fix the error, then run `sst validate` again; `sst explain <CODE>` explains a code
```

An `E` or `W` line gives the code, the `file:line:col` of the value at fault, the artifact,
and the message; `fix:` is the suggested fix, and a `cascade` line is a follow-on finding the
fix should clear. Info and baselined findings are only counted in the first line.

```bash
sst -o json validate --no-snowflake-syntax-check
```

prints every field instead: each entry of `diagnostics` has `code`, `severity`, `message`,
`location` (file, line, column), and a `help_url`; `summary` counts them by severity,
`exit_code` is the process exit code, and `next_steps` is what to do next. Work from one of
these, not from scraped `table` text.

## Explain before you change anything

```bash
sst explain SST-VAL116
```

prints, offline, when the code is raised, its message and suggested fix, and its docs link.
The full reference is @SST_DOCS@/reference/error-codes.md. A code from SST 0.3 still
explains, and names the 1.0 code it became.

## Work in this order

1. **Fix errors before warnings, and the first error of an artifact first.** An artifact
   with an error is still checked, but it is not attached to views or built, so fixing one
   error can reveal the next, and follow-on findings are downgraded to info (cascades).
   `--show-cascade` shows them.
2. **Fix the YAML the `location` names**, keeping to the surrounding style, then run
   `sst format` and the same validate again.
3. **Re-run until clean**, then compile: `sst compile --emit-ddl target/ddl`.
4. **Stay offline** with `--no-snowflake-syntax-check`. A connected validate also has
   Snowflake compile every expression, and checks against the data, such as join keys that
   repeat (`SST-VAL212`). Run it when the user is ready to connect.

## Common codes

| Code | What it means | Usual fix |
|---|---|---|
| `SST-REF001` | `ref()` names no model, snapshot or seed in the dbt manifest | Check the model name; run `dbt deps` if it is in a package. |
| `SST-MEM005` | A member attached to no view | Add the models its `ref()` calls name to one view's `tables:`. |
| `SST-MEM002` | A member names no table | Name its columns or tables with `{{ ref() }}`. |
| `SST-PRS126` | A key SST no longer reads | Delete it; the message says where the value comes from now. |
| `SST-VAL309` | A column a view exposes has no `data_type` | Add `data_type` in the model YAML. |
| `SST-VAL005` | A description does not say when to use it | Say what it answers, and when not to use it. |
| `SST-VAL101` | A metric expression does not aggregate | Wrap it in an aggregate, or make it a fact. |
| `SST-VAL116` | Two join paths are possible | Add `using_relationships:` to the metric. |
| `SST-VAL311` | An equality join's right table declares no key | Add `primary_key` in the model's `meta.sst`. |
| `SST-VAL405` | A boolean filter has no label | Add `labels: [filter]`. |
| `SST-PRS004` | A key SST does not read | Check the spelling against the docs; it is ignored. |
| `SST-VAL551` | An agent run refused a model `snowflake.orchestration_models` lists | Remove the model from the list; don't add one that hasn't run in an agent. |

## The baseline file is not a fix

`.sst/baseline.json` records warnings a project knows about and has not fixed yet: a
baselined warning is counted, never shown, and never blocks, until the baseline expires. Only
a demotable code below error can be baselined. Errors cannot.

- **Do not add to the baseline to get a run to pass.** Use `sst baseline add` only when the
  user asks, for a warning they decide to accept for now, and say which code and how many.
- `sst baseline show` lists what is there; `sst baseline prune` removes entries nothing
  matches any more, which is always safe to suggest.
- `--no-baseline` shows every diagnostic for one run; `--show-baselined` shows the ones it
  holds.

## When the project must ship with one artifact broken

`sst compile --partial`, then `sst plan --partial` and `sst apply --partial`, go ahead
with everything the errors do not touch, and still exit 1. Suggest it; never run the apply
yourself without being asked.
