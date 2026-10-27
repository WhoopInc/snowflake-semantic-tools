# Join paths and fan-out

<!--
Bundled file one of two, reached from `../SKILL.md` by a relative Markdown link,
which is the form the bundle scan follows (SST-VAL808 when a link does not resolve).
The catalog channel publishes it beside `SKILL.md` as `reference__join-paths.md`; the
stage channel keeps it here.
-->

## Why the ASOF join does not fan out

`order_items` is a finer grain than `orders`: one order has many line items. A
naive equality join from `orders` to `order_items` multiplies every order-level
fact by its line-item count, so `SUM(order_total)` over that join returns a number
several times larger than revenue and nothing in the SQL looks wrong.

The ASOF relationship is declared in the other direction -- `order_items` is the
left table, `orders` the right -- and adds a time probe:

```
{{ ref('order_items', 'order_id') }} = {{ ref('orders', 'order_id') }}
{{ ref('order_items', 'occurred_at') }} >= {{ ref('orders', 'ordered_at') }}
```

Each line item resolves to exactly one order row, so order-level facts are not
duplicated. At most one ASOF condition is permitted per relationship (R007).

## When a count looks too high

Work through these in order:

1. **Is the metric table-scoped on the finer grain?** `line_item_count` counts
   `order_items`, so it is larger than `order_count` by design.
2. **Did the question span both views?** A count from `JAFFLE_MENU` is line items;
   the same word in `JAFFLE_SALES` means orders.
3. **Was a non-additive fact summed?** `supply_cost` across months is the common
   one. Use `total_supply_cost`.
4. **Was a filter expected but absent?** Revenue questions usually mean completed
   orders; `is_completed_order` is not applied unless asked for.

## Path selection

`order_items` reaches `products` directly and `orders` through the ASOF
relationship. Where more than one route exists from a metric's table, name the path
with `using_relationships` rather than letting it be inferred -- `line_item_count`
does exactly that.
