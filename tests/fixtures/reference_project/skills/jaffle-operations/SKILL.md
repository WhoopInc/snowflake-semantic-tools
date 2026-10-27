---
name: jaffle-operations
description: |-
  Use when a question is about running the jaffle analytics surface rather than
  querying it -- which pricing period is in effect, how to reconcile a margin figure
  against supply snapshots, or how to check whether a supply month is complete.
  Invoke for terms like pricing period, effective date, supply snapshot, month
  close, reconcile, backfill. Do NOT invoke for ordinary analytical questions about
  orders or products; those belong to the jaffle-semantics skill.
---

# Jaffle operations

<!--
Publishing on both channels. A project configures at least one of `catalog` and
`stage` (SST-VAL817); this skill reaches both, with a bundle and a script.

Both channels carry the folder as authored: the catalog extension and the stage tree
hold the same paths, `reference/` and the nested `supply-backfill/` skill included.
This skill carries a bundle, a script, and a nested skill, so the goldens pin all
three shapes at their authored paths.

`supply-backfill/` is a skill of its own, nested here because it is one step of the
month close. It publishes as its own extension, and this folder's bundle carries it
too, as it sits on disk.

The script sits at the folder root. A skill keeps its scripts in the same folder as
`SKILL.md`, not under a subdirectory, so `check-supply-month.sql` is a sibling of this
file while prose reference material lives under `reference/`. The asymmetry is
deliberate: it is the layout the Agent Skills format describes.
-->

Operational questions about the jaffle surface. This skill is about the mechanics of
the data, not about what it says.

## Which pricing period is in effect

Pricing periods are half-open: `effective_start_at` is inclusive,
`effective_end_at` is exclusive. An order at exactly `effective_end_at` belongs to
the **next** period, not the one ending. That is what makes adjacent periods
non-overlapping and why the range relationship renders `EXCLUSIVE`.

`pricing_periods` declares `distinct_range` in its dbt model's `meta.sst`, which
is a requirement before any range relationship may point at it (R009).

## Reconciling a margin figure

`gross_margin` is `total_line_item_revenue` minus `total_supply_cost`. If a margin
figure looks wrong, check in this order:

1. **Is the supply month complete?** Run `check-supply-month.sql`. A partial
   month understates cost and overstates margin.
2. **Was supply cost summed across months?** It must not be. `total_supply_cost`
   declares `snapshot_month` non-additive; aggregating the column directly does not.
3. **Are both figures in cents?** Margin in cents against revenue in dollars is a
   100x error that looks plausible.

## Further reference

- [Month close checks](reference/month-close.md) -- what has to be true before a
  supply month is treated as final.
- [Backfilling a supply month](supply-backfill/SKILL.md) -- when a check finds a
  month incomplete. See month-close.md for the checks it reruns.
