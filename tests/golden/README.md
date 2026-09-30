# Goldens

The files under `expected/` are owned by this repo: each is what SST renders for the
reference fixture, and each DDL golden was created in Snowflake as written. Regenerate
one only when a change to the engine or the fixture is meant to change it, and review the
diff like code.

## File shape

Each golden is a leading block of `--` comment lines carrying provenance -- what the view
exercises and what it deliberately omits -- then a blank line, then the DDL through end of
file.

The comparison contract is therefore: **the golden is everything from the first line that is
not a comment or blank.** The header is documentation and is not rendered by the engine.

## What the header is NOT

`jaffle_minimal.sql` says so in its own words, and it is worth repeating here because it is
the kind of thing that gets lost: these files are **not** what `GET_DDL` returns and must not
be compared against it raw. `GET_DDL` lowercases every keyword, rewrites
`WITH SYNONYMS ('a', 'b')` into `with synonyms=('a','b')` while leaving
`sample_values ('x', 'y')` spaced and un-`=`-ed, and drops `COPY GRANTS` entirely. Two clauses
authored alike come back unalike, so a drift normaliser cannot be inferred from just one.
