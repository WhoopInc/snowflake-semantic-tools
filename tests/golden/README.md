# Goldens

`expected/ddl/*.sql` were copied once from the SST 1.0 design work's hand-derived goldens
and are **owned by this repo thereafter**. They were hand-derived there, and four different
`byte_length` pairs ended up in circulation as a direct result (review MEDIUM 14). Deriving
them from executed DDL is the point of moving them here.

## File shape

Each golden is a leading block of `--` comment lines carrying provenance -- what the view
exercises, what it deliberately omits, which drift findings it records -- then a blank line,
then the DDL through end of file.

The comparison contract is therefore: **the golden is everything from the first line that is
not a comment or blank.** The header is documentation and is not rendered by the engine.

## What the header is NOT

`jaffle_minimal.sql` says so in its own words, and it is worth repeating here because it is
the kind of thing that gets lost: these files are **not** what `GET_DDL` returns and must not
be compared against it raw. `GET_DDL` lowercases every keyword, rewrites
`WITH SYNONYMS ('a', 'b')` into `with synonyms=('a','b')` while leaving
`sample_values ('x', 'y')` spaced and un-`=`-ed, and drops `COPY GRANTS` entirely. Two clauses
authored alike come back unalike, so a drift normaliser cannot be inferred from just one.
