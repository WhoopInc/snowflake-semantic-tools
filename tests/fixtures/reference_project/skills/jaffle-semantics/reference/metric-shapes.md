# Metric shapes

<!--
BUNDLED FILE TWO OF TWO. A second bundled file exists so the flattened golden
asserts a COLLISION-FREE flatten of more than one file: with a single bundled file,
flattening is indistinguishable from moving it. See `../SKILL.md`.
-->

## Table-scoped versus derived

**Table-scoped** metrics aggregate a column and render into their owning table's
`METRICS` clause. **Derived** metrics combine other metrics and render at view
level. The distinction is visible in the expression: `{{ ref(...) }}` is
table-scoped, `{{ metric(...) }}` is derived.

A derived metric may **not** contain a window function. SST 1.0 also requires a
table-scoped metric to have an aggregate at the expression root, so window calculations
belong in the dbt model where they can be tested before the semantic view consumes them.

## What is derived from what

| Derived metric | Built from | Depth |
|---|---|---|
| `revenue_per_order` | `total_revenue`, `order_count` | 1 |
| `revenue_per_customer` | `total_revenue`, `customer_count` | 1 |
| `gross_margin` | `total_line_item_revenue`, `total_supply_cost` | 1 |
| `gross_margin_rate` | `gross_margin`, `total_line_item_revenue` | 2 |

`gross_margin_rate` is the only metric at depth 2 -- derived from a metric that is
itself derived. It exists so the expansion depth guard is observable: the guard runs
before descent, so without a nested case a working guard and an absent one look the
same.

## Division

Every ratio uses `DIV0`, not `/`. A derived ratio over a metric whose value can be
zero is the most common division-by-zero site in a semantic layer, and Snowflake
provides a total function for it. `gross_margin` is the one derived metric using
plain subtraction, because subtraction has no undefined case.

## Reading a rate

Rates are decimals between zero and one, never percentages. `gross_margin_rate`
returns `0.42`, not `42`.
