# Goldens

The files under `expected/` are owned by this repo: each is what SST renders for the
reference fixture under its `dev` target, one directory per artifact type -- `ddl/`
(semantic views), `tool/`, `agent/`, `eval/`, `skill/`, `plugin/`, `profile/` -- plus
`enrich/`. Regenerate one only when a change to the engine or the fixture is meant to
change it, and review the diff like code.

Two of the DDL goldens, `jaffle_product_costs.sql` and `jaffle_customer_orders.sql`,
render from `tests/fixtures/scoped_views/semantic_views.yml` placed in a copy of the
reference project at `semantic_models/semantic_views/scoped/`; the others render from
the reference project as it is. The reference project's own semantic view goldens have
been created in Snowflake as written; each header says which.

`expected/enrich/` is different: it holds whole YAML files, as `sst enrich --include all`
leaves the reference project's files under the warehouse and Cortex answers that
`unit/test_golden_enrich.py` scripts by hand. Each file sits at its path in the project and
is compared byte for byte; none carries a header. `sst test --suite golden --update-golden`
does not write them: when enrich's output is meant to change, run that test to see the diff
and edit the golden to match.

## How a golden is compared

`sst test --suite golden` compares the rendered text with the golden exactly, after two
normalisations on both sides: trailing whitespace at the end of the file is dropped and one
final newline kept, and a `GIT_<commit>` name carries `GIT_0000000` in place of the commit.
`--update-golden` writes a golden in that same form, and the `unit/test_golden_*.py` tests hold
the engine to the same files.

## File shape

Each semantic view golden in `ddl/` is a leading block of `--` comment lines carrying
provenance -- what the view exercises and what it deliberately omits -- then a blank line,
then the DDL through end of file. Every other golden, the tool's DDL included, has no header.

The comparison contract for a DDL golden (`ddl/` and `tool/`) is therefore: **the golden is
everything from the first line that is not a comment or blank.** The header is
documentation and is not rendered by the engine; `--update-golden` keeps the header a golden
already has.

## What the header is NOT

`jaffle_minimal.sql` says so in its own words, and it is worth repeating here because it is
the kind of thing that gets lost: these files are **not** what `GET_DDL` returns and must not
be compared against it raw. `GET_DDL` lowercases every keyword, rewrites
`WITH SYNONYMS ('a', 'b')` into `with synonyms=('a','b')` while leaving
`sample_values ('x', 'y')` spaced and un-`=`-ed, and drops `COPY GRANTS` entirely. Two clauses
authored alike come back unalike, so a drift normaliser cannot be inferred from just one.
