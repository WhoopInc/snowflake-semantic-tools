---
name: supply-backfill
description: |-
  Use when a jaffle supply month is incomplete and must be backfilled before its
  margin can be reported. Invoke for terms like backfill, missing snapshot, late
  supply data. Do NOT invoke to answer margin questions; that is jaffle-operations.
---

# Backfilling a supply month

<!--
A NESTED SKILL. Its folder sits inside `jaffle-operations/`, which publishes it as part
of its own bundle; this folder also publishes alone, as the `SUPPLY_BACKFILL`
extension. Its sidecar sits in its own `reference/`, which the parent's `reference/`
does not shadow.
-->

1. Load the missing snapshots for the month, one row per product.
2. Rerun the checks in [the backfill checklist](reference/checklist.md).
3. Report margin for the month only once every check passes.
