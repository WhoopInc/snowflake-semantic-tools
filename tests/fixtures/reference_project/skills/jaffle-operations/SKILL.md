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
THIS SKILL IS THE K2xx CASE: DUAL-CHANNEL PUBLISHING. K201 requires at least one
channel (`catalog` or `stage`) to be configured; this skill configures BOTH, which
is what the K2xx family is about and what neither of the other two skills reaches.

WHY DUAL-CHANNEL NEEDS ITS OWN SKILL RATHER THAN A FLAG ON AN EXISTING ONE. The two
channels do not carry the same payload:

  catalog   FLAT. Snowflake does not search subdirectories (K101), so a bundle is
            collapsed and a name collision between two bundled files is fatal here
            and invisible on the other channel.
  stage     TREE PRESERVED. The bundle uploads as authored.

A skill on one channel cannot assert the divergence. This one carries a bundle AND a
script, so the flatten has two distinct file types to collapse, and the goldens
assert that the catalog payload and the stage payload differ in path while agreeing
in content.

THE SCRIPT IS WHY `scripts/` SITS AT THE FOLDER ROOT. K001 puts scripts in the same
folder as `SKILL.md`, not under a subdirectory -- so `check-supply-month.sql` is a
sibling of this file, while prose reference material lives under `reference/`. That
asymmetry is authored deliberately: it is the layout the rule describes.
-->

Operational questions about the jaffle surface. This skill is about the mechanics of
the data, not about what it says.

## Which pricing period is in effect

Pricing periods are half-open: `effective_start_at` is inclusive,
`effective_end_at` is exclusive. An order at exactly `effective_end_at` belongs to
the **next** period, not the one ending. That is what makes adjacent periods
non-overlapping and why the range relationship renders `EXCLUSIVE`.

`pricing_periods` declares `distinct_range` in `JAFFLE_MENU`'s `table_config`, which
is a requirement before any range relationship may point at it (R009), and which is
one of the three things that cannot be derived from a dbt model.

## Reconciling a margin figure

`gross_margin` is `total_line_item_revenue` minus `total_supply_cost`. If a margin
figure looks wrong, check in this order:

1. **Is the supply month complete?** Run `scripts/check-supply-month.sql`. A partial
   month understates cost and overstates margin.
2. **Was supply cost summed across months?** It must not be. `total_supply_cost`
   declares `snapshot_month` non-additive; aggregating the column directly does not.
3. **Are both figures in cents?** Margin in cents against revenue in dollars is a
   100x error that looks plausible.

## Further reference

- [Month close checks](reference/month-close.md) -- what has to be true before a
  supply month is treated as final.
