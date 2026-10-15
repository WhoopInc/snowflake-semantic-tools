---
name: jaffle-catalogue
description: |-
  Use when a question needs only the jaffle menu catalogue -- which products exist,
  what type each is, and what each lists for. Invoke for terms like menu, product,
  SKU, list price, food versus drink. Do NOT invoke for anything about orders,
  customers, revenue or margin; those need JAFFLE_SALES or JAFFLE_MENU and are
  covered by the jaffle-semantics skill.
# This skill certifies itself alone: its own config: beats the folder routes and
# the skills: root, whose +certified is false.
config:
  certified: true
---

# Jaffle menu catalogue

<!--
The smallest skill: no bundle, no `reference/` directory, no scripts -- only
`SKILL.md` at the folder root.

Do not add a bundle to this skill. It plays the role `jaffle_minimal` plays among the
views and `total_revenue_all_time` among the verified queries: a publisher that walks
a bundle unconditionally, or emits a flattened payload for a skill with nothing to
flatten, fails its golden (`tests/golden/expected/skill/jaffle-catalogue.bundle.json`)
and no other.
-->

Use `JAFFLE_MINIMAL` for catalogue questions. It carries `products` and nothing
else, so there is no join to reason about and no fan-out to avoid.

## What it answers

- Which products exist, and how many.
- Whether a product is food (`jaffle`) or drink (`beverage`).
- What a product lists for. `list_price` is in cents -- divide by 100.

## What it does not answer

Anything requiring an order: units sold, revenue, margin, or which customers bought
what. `product_count` exists in `JAFFLE_MENU` as well, joined to line items, and
that is the view to use when the question is about sales rather than the catalogue.
