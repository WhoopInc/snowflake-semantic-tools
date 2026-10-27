# Goldens

The files under `expected/` are owned by this repo: each is what SST renders for the
reference fixture under its `dev` target, one directory per artifact type -- `ddl/`
(semantic views), `tool/`, `agent/`, `cowork_profile/` (a Snowflake CoWork profile, a
Snowflake private preview), `eval/`, `skill/`, `plugin/`, `profile/` -- plus `enrich/`.
Regenerate one only when a change to the engine or the fixture is meant to change it, and
review the diff like code.

Two of the DDL goldens, `jaffle_product_costs.sql` and `jaffle_customer_orders.sql`,
render from `tests/fixtures/scoped_views/semantic_views.yml` placed in a copy of the
reference project at `semantic_models/semantic_views/scoped/`; the others render from
the reference project as it is. `unit/test_golden_ddl.py` compares those two, and
`python -m tests.helpers.update_goldens` rewrites them from such a copy. The reference
project's own semantic view goldens have been created in Snowflake as written; each header says
which.

`expected/enrich/` is different: it holds whole YAML files, as `sst enrich --include all`
leaves the reference project's files under the warehouse and Cortex answers that
`unit/test_golden_enrich.py` scripts by hand. Each file sits at its path in the project and
is compared byte for byte; none carries a header. `python -m tests.helpers.update_goldens`
does not write them: when enrich's output is meant to change, run that test to see the diff
and edit the golden to match.

`expected/cli/` holds what `sst` itself prints: a few command lines on the reference project
(validate with and without errors, list, a usage error, help, a plan on a fake Snowflake that
holds nothing, and the agent document), each as a transcript of its exit code, stdout and
stderr, on up to four terminals (`color`, `plain`, `ascii`, `narrow`), with the project's path
and every run's seconds normalized. `unit/cli/test_cli_goldens.py` compares them;
`python -m tests.helpers.cli_goldens` rewrites the ones that differ, and is refused in CI.

## How a golden is compared

`unit/test_goldens.py` compiles the reference project and compares every artifact's rendered
text with its golden exactly, after two normalizations on both sides: trailing whitespace at
the end of the file is dropped and one final newline kept, and a `GIT_<commit>` name carries
`GIT_0000000` in place of the commit.
`python -m tests.helpers.update_goldens`, run from the repository root, writes a golden in that
same form (it refuses when `$CI` is set to anything but empty, `0`, `false` or `no`), and the
`unit/test_golden_*.py` tests hold the engine to the same files. The comparison and the rewrite
live in `tests/helpers/goldens.py`.

## File shape

Each semantic view golden in `ddl/` is a leading block of `--` comment lines carrying
provenance -- what the view exercises and what it deliberately omits -- then a blank line,
then the DDL through end of file. Every other golden, the tool's DDL included, has no header.

The comparison contract for a DDL golden (`ddl/` and `tool/`) is therefore: **the golden is
everything from the first line that is not a comment or blank.** The header is
documentation and is not rendered by the engine; `update_goldens` keeps the header a golden
already has.

## What the header is NOT

`jaffle_minimal.sql` says so in its own words, and it is worth repeating here because it is
the kind of thing that gets lost: these files are **not** what `GET_DDL` returns and must not
be compared against it raw. `GET_DDL` lowercases every keyword, rewrites
`WITH SYNONYMS ('a', 'b')` into `with synonyms=('a','b')` while leaving
`sample_values ('x', 'y')` spaced and un-`=`-ed, and drops `COPY GRANTS` entirely. Two clauses
authored alike come back unalike, so a drift normalizer cannot be inferred from just one.
