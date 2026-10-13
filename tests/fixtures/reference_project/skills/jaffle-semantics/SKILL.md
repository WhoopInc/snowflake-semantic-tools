---
name: jaffle-semantics
description: |-
  Use when writing or reviewing a question against the jaffle semantic views --
  JAFFLE_SALES, JAFFLE_MENU or JAFFLE_MINIMAL. Covers which view answers which
  question, how the three join kinds behave (equality, ASOF, range), why a line-item
  count joined to order state does not fan out, why supply cost must never be summed
  across months, and how the two view variables change what a metric returns. Invoke
  for terms like order, customer, jaffle, menu item, revenue, AOV, margin, supply
  cost, pricing period, ASOF, fan-out, non-additive, or "which view should I use".
  Do NOT invoke for menu catalogue content -- that is the search tool -- and do NOT
  invoke for questions about an individual named customer.
---

# Jaffle semantics

<!--
A skill is Markdown rather than YAML: the Agent Skills format, read by Cortex
Agents, by CoCo Desktop, and by tools outside Snowflake.

This skill carries a bundle: two files under `reference/`, which is what makes the
catalog channel's flattening observable. `SKILL.md` sits at the folder root and Cortex
Agents do not search subdirectories, so the catalog channel publishes each bundled
file beside it as `reference__<name>.md`, with every link rewritten to match
(SST-VAL810); the stage channel keeps the tree. The catalog payload is pinned by
`tests/golden/expected/skill/jaffle-semantics.bundle.json`.
-->

## Which view answers which question

| Question is about | View | Why |
|---|---|---|
| order volume, order value, tax, revenue by location, new vs returning | `JAFFLE_SALES` | it joins orders to customers and locations |
| product mix, units sold, line-item revenue, margin, supply cost, pricing periods | `JAFFLE_MENU` | it joins line items to products, orders, pricing periods and supplies |
| menu products alone, with no order context | `JAFFLE_MINIMAL` | one table, no joins |

A question needing **both** customer attributes and product detail spans two views
and cannot be answered from either. Say so rather than answering from whichever
view is to hand.

## The three join kinds, and what each protects

- **Equality** (`orders` to `customers`, `orders` to `locations`, `order_items` to
  `products`, `supplies` to `products`). Ordinary fact-to-dimension. Safe because
  every right-hand side declares a primary key.
- **ASOF** (`order_items` to `orders`). Matches a line item to the order state in
  effect **when the item was recorded**, not to the order's current state. This is
  what stops a later state change silently rewriting history.
- **Range** (`orders` to `pricing_periods`). `BETWEEN start AND end EXCLUSIVE`. The
  upper bound is open, which is what makes adjacent periods non-overlapping.

## Two things that will give a wrong answer if ignored

**Supply cost is not additive across months.** `supplies` restates cost per product
per month. Summing `supply_cost` across months double-counts. Always use the
`total_supply_cost` metric, which declares `snapshot_month` as non-additive.

**Monetary columns are in cents.** Divide by 100 before presenting a currency
figure. Every metric returns cents.

## The view variables

`JAFFLE_SALES` declares two variables, and they change what a metric returns:

- `large_order_cents` (default 1000) -- the threshold `large_order_count` and the
  `is_large_order` filter compare against.
- `tax_inclusive` (default false) -- whether `revenue_on_policy` measures
  `order_total` or `subtotal`.

A question about "large orders" is answered relative to the variable, not to a
number in the question. Say which threshold was used.

## Further reference

- [Join paths and fan-out](reference/join-paths.md) -- why the ASOF join does not
  fan out, and what to do when a count looks too high.
- [Metric shapes](reference/metric-shapes.md) -- table-scoped versus derived, and
  which metrics are derived from which.
