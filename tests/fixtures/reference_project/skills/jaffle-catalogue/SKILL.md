---
name: jaffle-catalogue
description: |-
  Use when a question needs only the jaffle menu catalogue -- which products exist,
  what type each is, and what each lists for. Invoke for terms like menu, product,
  SKU, list price, food versus drink. Do NOT invoke for anything about orders,
  customers, revenue or margin; those need JAFFLE_SALES or JAFFLE_MENU and are
  covered by the jaffle-semantics skill.
---

# Jaffle menu catalogue

<!--
THIS SKILL IS THE K0xx CASE, AND ITS VALUE IS WHAT IT DOES NOT HAVE. No bundle, no
`reference/` directory, no scripts, no second channel -- just `SKILL.md` at the
folder root, which is what K001 requires.

DO NOT ADD A BUNDLE TO THIS SKILL. It is the negative-space assertion for the skill
artifact, the same role `jaffle_minimal` plays among the views and
`total_revenue_all_time` plays among the verified queries. A publisher that walks a
bundle unconditionally, or that emits a flattened-catalog payload for a skill with
nothing to flatten, is caught here and nowhere else.
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
