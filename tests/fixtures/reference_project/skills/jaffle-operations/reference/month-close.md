# Month close checks

<!--
THE SECOND BUNDLED FILE IN THE DUAL-CHANNEL SKILL. Its existence is what gives the
flattened catalog payload two files from two different origins to collapse -- this
one from `reference/`, and `check-supply-month.sql` from the folder root. With only
one, flattening cannot be distinguished from a rename.
-->

A supply month is treated as final when all of the following hold. Until they do,
any margin figure spanning the month is provisional.

## 1. Every product sold has a snapshot

Run `../check-supply-month.sql`. It returns one row per product that sold in the
month with no matching supply snapshot. An empty result is the pass condition.

## 2. The snapshot grain is intact

`supplies` is one row per product per month. If the composite uniqueness test on
`(product_id, snapshot_month)` fails, the month is not closeable -- and more
importantly `total_supply_cost` is wrong in a way no ref resolution or SQL
compilation would catch. That dbt test is what makes the non-additive declaration
true rather than merely asserted.

## 3. No order sits outside a pricing period

Every order should fall inside exactly one period. An order at exactly an
`effective_end_at` boundary belongs to the next period, since the upper bound is
exclusive. An order matching **no** period means the periods have a gap, which the
`distinct_range` declaration asserts cannot happen -- so a gap is a data defect
rather than a query mistake.

## What closing does not mean

Closing a supply month does not freeze orders. Orders remain mutable, which is why
line items attach to order state by an ASOF join rather than by equality: a later
state change does not rewrite what a closed month reported.
