---
description: Check that a supply month is closed before its cost is reported.
allowed-tools:
  - snowflake_sql_execute
---
Before reporting supply cost for a month, confirm the month is closed.

Run the check in the jaffle-operations skill, `check-supply-month.sql`, for the
month in question. If it reports the month open, say so and stop: supply cost is
non-additive across months, and an open month's figure will still change.
