# Snowflake behaviour SST depends on

SST renders and publishes Snowflake objects, so its code encodes how the
Snowflake server behaves. In several places that behaviour contradicts
Snowflake's published documentation, or the documentation says nothing about
it. Code written against the server can therefore look wrong to someone reading
the docs. A maintainer who "fixes" it to match the docs reintroduces a defect the
code was written to avoid.

This page records each such behaviour: what the docs say, what the server does,
how strongly SST's own tests hold the dependency in place, where the code relies
on it, and how to check it again. The working rule is **docs for the grammar,
probe for whether the server agrees**. The documentation is the cheapest source
of a statement to try. Only running it shows whether the server accepts it, keeps
it, and answers queries over it.

## How to read an entry

Each behaviour is a `###` heading with four fields:

- **Docs say**: the claim in Snowflake's published documentation, or the fact
  that it says nothing.
- **Server does**: what Snowflake does when the statement runs.
- **Evidence**: one of two grades.
  - `Enforced`: a code site and an offline test pin SST's side of the
    dependency, so changing the code to follow the docs fails a test. Both are
    linked. The test runs offline: it proves SST still depends on the behaviour,
    not that the server still has it.
  - `Observed`: the code relies on the behaviour, but no automated check would
    fail if the code were changed. The entry gives a probe to run instead.
- **SST depends on it at**: the code, tests, and user docs that rely on it.

Probe sketches use placeholder names. Run them only in a disposable scratch
schema, as [Re-checking a behaviour](#re-checking-a-behaviour) describes.

## Semantic views

### MAX_STALENESS comes after the AI clauses

**Docs say**: the `CREATE SEMANTIC VIEW` grammar orders the trailing clauses
`COMMENT`, `MAX_STALENESS`, `AI_SQL_GENERATION`, `AI_QUESTION_CATEGORIZATION`,
`AI_VERIFIED_QUERIES`, `WITH TAG`, `COPY GRANTS`.

**Server does**: it rejects that order for a view that carries both
`MAX_STALENESS` and an AI clause, reporting `unexpected 'AI_SQL_GENERATION'`.
It accepts `COMMENT`, `AI_SQL_GENERATION`, `AI_QUESTION_CATEGORIZATION`,
`AI_VERIFIED_QUERIES`, `MAX_STALENESS`, `WITH TAG`, `COPY GRANTS`.
Two statements that differ only in where the `MAX_STALENESS` line sits show it.

**Evidence**: `Enforced`.
`test_every_clause_appears_in_the_verified_order` in
[test_render_semantic_view.py](../../tests/unit/test_render_semantic_view.py)
pins the order. The `jaffle_sales` golden, the only one carrying
`MAX_STALENESS`, has been created in Snowflake, and
[test_golden_ddl.py](../../tests/unit/test_golden_ddl.py) compares the
renderer's output with it byte for byte.

**SST depends on it at**: `_trailing_clauses` and the module docstring in
[semantic_view.py](../../snowflake_semantic_tools/domain/render/semantic_view.py);
[jaffle_sales.sql](../../tests/golden/expected/ddl/jaffle_sales.sql); the clause
list in [the semantic views guide](../guides/semantic-views.md#checking-the-rendered-view).

### MAX_STALENESS takes a quoted number of seconds

**Docs say**: `MAX_STALENESS = <integer>`, minimum 120.

**Server does**: it accepts `MAX_STALENESS = '300 seconds'`, the form SST
renders. The documented minimum holds: SST-CFG023 refuses less than 120, as a
view's `config.max_staleness` or as `semantic_views.+max_staleness`. Whether
the server also accepts a bare integer is not recorded here.

**Evidence**: `Enforced`. The `jaffle_sales` golden carries
`MAX_STALENESS = '300 seconds'` and was created in Snowflake;
[test_golden_ddl.py](../../tests/unit/test_golden_ddl.py) compares it.
[test_sst_cfg023.py](../../tests/codes/cfg/test_sst_cfg023.py) covers the minimum.

**SST depends on it at**: `_build_view` in
[build.py](../../snowflake_semantic_tools/adapters/yaml/semantic/build.py), which
turns the resolved integer into `'<n> seconds'`; `_trailing_clauses` in
[semantic_view.py](../../snowflake_semantic_tools/domain/render/semantic_view.py);
SST-CFG023 in
[cfg.py](../../snowflake_semantic_tools/domain/diagnostics/specs/cfg.py), raised by
`value_problem` in [config.py](../../snowflake_semantic_tools/domain/validate/config.py).

### A variable's COMMENT needs `=`; the AI clauses take none

**Docs say**: the variable production ends `[ COMMENT '<description>' ]`, with
no `=`. The view-level `AI_SQL_GENERATION '<text>'` and
`AI_QUESTION_CATEGORIZATION '<text>'` also take none, and there the docs are
right.

**Server does**: `COMMENT '<text>'` on a variable is a syntax error, reported at
the string literal rather than at `COMMENT`, so it reads as a bad string.
`COMMENT = '<text>'` is accepted. An `=` after either AI clause is a syntax
error. The rule is per clause and does not generalise in either direction.

**Evidence**: `Enforced`. `test_variable_default_is_rendered_verbatim` in
[test_render_semantic_view.py](../../tests/unit/test_render_semantic_view.py)
asserts `COMMENT = 'c'` on a variable, and the `jaffle_sales` golden pins both
AI clauses without `=`.

**SST depends on it at**: `render_variable` and `_trailing_clauses` in
[semantic_view.py](../../snowflake_semantic_tools/domain/render/semantic_view.py).

### Filters, verified queries and custom instructions are not clauses

**Docs say**: the grammar has no `FILTERS (...)`, `VERIFIED QUERIES (...)` or
`CUSTOM INSTRUCTIONS (...)` clause. Here the docs and the server agree. The entry
exists because SST's YAML keys `filters`, `verified_queries` and
`custom_instructions` make those clause names look natural.

**Server does**: it rejects all three at parse time. A filter is a dimension
with `LABELS = (FILTER)` before `AS`. Verified queries are `AI_VERIFIED_QUERIES`.
Custom instructions become the `AI_SQL_GENERATION` and
`AI_QUESTION_CATEGORIZATION` text.

**Evidence**: `Enforced`. `test_golden_has_no_ungrammatical_clause_heads` in
[test_golden_ddl.py](../../tests/unit/test_golden_ddl.py);
`test_filter_label_precedes_AS` and
`test_filters_render_inside_DIMENSIONS_not_their_own_clause` in
[test_render_semantic_view.py](../../tests/unit/test_render_semantic_view.py).

**SST depends on it at**: the module docstring and `render_column` in
[semantic_view.py](../../snowflake_semantic_tools/domain/render/semantic_view.py).

### SAMPLE_VALUES and IS_ENUM are native member clauses

**Docs say**: nothing. Neither clause appears in the dimension or fact
productions.

**Server does**: it accepts both, keeps them, and returns them from `GET_DDL`.
The spelling is bare, `SAMPLE_VALUES ('a', 'b')` and `IS_ENUM`, with no `WITH`
and no `=`. `SAMPLE_VALUES` must come before `IS_ENUM`. `SAMPLE_VALUES` is
accepted on dimensions and facts. `IS_ENUM` is accepted on dimensions only: on a
fact it fails with `Only dimensions can specify the is_enum property`.

**Evidence**: `Enforced` for the spelling and order. `test_full_modifier_order`
in [test_render_semantic_view.py](../../tests/unit/test_render_semantic_view.py)
pins them, and the `jaffle_sales` golden carries both on dimensions and
`SAMPLE_VALUES` on facts. The dimensions-only rule is `Observed`. `enrich` never
writes `is_enum` on a fact, and a hand-authored `is_enum: true` on a fact column
is refused before it renders (SST-VAL335).
[test_sst_val335.py](../../tests/codes/val/test_sst_val335.py). Probe:

```sql
CREATE SEMANTIC VIEW <db>.<schema>.probe_view
  TABLES (T AS <db>.<schema>.probe_t PRIMARY KEY (ID))
  FACTS (T.AMOUNT AS T.AMOUNT SAMPLE_VALUES ('1', '2') IS_ENUM)
  METRICS (T.TOTAL AS SUM(T.AMOUNT));
-- Expect: refused, naming is_enum. Remove IS_ENUM and it is accepted.
```

**SST depends on it at**: `render_column` in
[semantic_view.py](../../snowflake_semantic_tools/domain/render/semantic_view.py);
[build.py](../../snowflake_semantic_tools/adapters/yaml/semantic/build.py), which
passes `is_enum` through for any column kind; `_sample_value_diagnostics` in
[validate/semantic/dbt.py](../../snowflake_semantic_tools/domain/validate/semantic/dbt.py).

### GET_DDL output does not re-execute

**Docs say**: `GET_DDL` returns "the text of the DDL statement that created the
object". Its optional third argument, `use_fully_qualified_names_for_recreated_objects`,
defaults to `FALSE`, so the name comes back unqualified unless asked otherwise.
Nothing says the statement is rewritten.

**Server does**: for a semantic view it returns a canonical form, not the
authored statement. Keywords are lowercased. `WITH SYNONYMS ('a', 'b')` returns
as `with synonyms=('a','b')`, while `SAMPLE_VALUES` keeps its spacing and gains
no `=`. `COPY GRANTS` is dropped. The trailing clauses come back in the
documented order, with `MAX_STALENESS` before the AI clauses, which the parser
rejects (see [above](#max_staleness-comes-after-the-ai-clauses)). So for a view
carrying both, `GET_DDL`'s own output does not parse. With the default name
form, it also fails in a session that has no current database.

**Evidence**: `Observed`. No test reads `GET_DDL` from a real account, and none
runs what it returns. Probe:

```sql
CREATE SEMANTIC VIEW <db>.<schema>.probe_view
  TABLES (T AS <db>.<schema>.probe_t PRIMARY KEY (ID))
  METRICS (T.N AS COUNT(T.ID))
  COMMENT = 'probe'
  AI_SQL_GENERATION 'probe'
  MAX_STALENESS = '300 seconds';
SELECT GET_DDL('SEMANTIC_VIEW', '<db>.<schema>.probe_view');
-- Then run the returned text in a fresh session with no current database.
-- Expect: it fails, first on the unqualified name and, once qualified, on
-- the clause order.
```

**SST depends on it at**: `get_ddl` in
[catalog.py](../../snowflake_semantic_tools/adapters/snowflake/connector/catalog.py),
which passes `TRUE` as the third argument so names come back fully qualified, and
`restorable_semantic_view` in [prior.py](../../snowflake_semantic_tools/app/prior.py),
which `plan --capture-prior` runs on a semantic view's text: it moves the trailing
clauses into the order the renderer uses (`TRAILING_CLAUSES` in
[semantic_view.py](../../snowflake_semantic_tools/domain/render/semantic_view.py))
and appends `COPY GRANTS`. Text of another shape is kept as returned, under a
comment saying what to fix by hand. The
[CI/CD guide](../guides/ci-cd.md#recording-what-an-update-replaces) says to run
the recorded text as it is only for a semantic view, and only for that form; since
this behaviour is `Observed`, so is that advice. Nothing here records whether
`GET_DDL`'s text for a search service, procedure or function re-executes, so the
guide says to review those before running them. The
[golden README](../../tests/golden/README.md) warns that a golden and a `GET_DDL`
read-back must not be compared raw.

### The derived-metric restrictions are not enforced by the server

**Docs say**: a derived metric's expression cannot use aggregations of metrics,
window functions, references to physical columns, or unaggregated facts or
dimensions. A derived metric can be used only by another derived metric.

**Server does**: for a window function in a derived metric, it accepts the view
at `validate_only` and at `CREATE`, then fails the first query with
`SQL execution internal error`, not a clean compile error. A created view is not
a working view. The boundary matters as much as the failure. A window function
in a table-scoped metric is legal and works. A window over a raw fact, rather
than over a metric or an aggregate, is refused at `CREATE` with a precise
message. So the server does enforce some window rules early. SST's note on
SST-VAL123 says the server enforces none of the derived-metric restrictions.
Only the window-function case has been probed to query time.

**Evidence**: `Enforced`. The checks are SST-VAL102 to SST-VAL107, none of them
demotable, and SST-VAL123 re-checks the compiled view.
[test_sst_val102.py](../../tests/codes/val/test_sst_val102.py) and
[test_sst_val123.py](../../tests/codes/val/test_sst_val123.py).
SST-VAL126 is the table-scoped counterpart, refusing a window whose argument is
neither a metric nor an aggregate:
[test_sst_val126.py](../../tests/codes/val/test_sst_val126.py).

**SST depends on it at**: `CRITICAL_METRIC_CODES` and `restriction_diagnostics`
in
[validate/semantic_view.py](../../snowflake_semantic_tools/domain/validate/semantic_view.py);
[val_metric.py](../../snowflake_semantic_tools/domain/diagnostics/specs/val_metric.py);
[the error-code reference](../reference/error-codes.md#sst-val123).

### Overlapping distinct ranges are accepted and double-count

**Docs say**: in the table that holds the ranges of a range join, "no two
ranges can overlap", declared with `CONSTRAINT ... DISTINCT RANGE`.

**Server does**: a probe created views over target rows whose ranges overlap,
with and without `UNIQUE` on the range pair. `CREATE` succeeded every time, and
an event inside two ranges was counted against both in a dimensioned query, with
no error. The metric-only total stayed right, so a spot check on the total
passes. `UNIQUE (start, end)` is no substitute: three distinct pairs can each
overlap a neighbour.

**Evidence**: `Enforced` for SST's own check. SST-VAL218 reads the target's rows
for one overlapping pair, but only when `validate` connects; offline it is
skipped and reported as skipped (SST-VAL020).
[test_sst_val218.py](../../tests/codes/val/test_sst_val218.py). The server's
acceptance is `Observed`. Probe:

```sql
CREATE TABLE <db>.<schema>.probe_ranges (ID INT, S DATE, E DATE);
INSERT INTO <db>.<schema>.probe_ranges
  SELECT 1, CURRENT_DATE, CURRENT_DATE + 2
  UNION ALL SELECT 2, CURRENT_DATE + 1, CURRENT_DATE + 3;
-- Create a view whose events table joins probe_ranges with
-- REFERENCES R (BETWEEN S AND E EXCLUSIVE), and R carrying
-- CONSTRAINT R_DISTINCT_RANGE DISTINCT RANGE BETWEEN S AND E EXCLUSIVE.
-- Query one event dated CURRENT_DATE + 1 by R.ID.
-- Expect: CREATE succeeds and the event appears under both IDs.
```

**SST depends on it at**: `_spot_checks` and `_overlap_query` in
[validate.py](../../snowflake_semantic_tools/app/validate.py). A range
relationship renders the range alone (`render_relationship` in
[semantic_view.py](../../snowflake_semantic_tools/domain/render/semantic_view.py)),
so SST-VAL202 refuses any condition beside it, and SST-VAL206 requires the target
to declare `distinct_range`.

### relationship_type and join_type are kept but not read

**Docs say**: semantic views do not require `join_type` or `relationship_type`.
The relationship type is inferred from the data and the primary-key definitions.

**Server does**: it accepts both fields and returns them unchanged on read-back,
while still inferring cardinality from the keys. A stale `many_to_one` beside a
key that says otherwise survives a round trip and reads as authoritative.

**Evidence**: `Enforced`. SST never renders either field. Declaring one is
SST-VAL211, and the removed `generation.emit_relationship_type` key is
SST-CFG043. [test_sst_val211.py](../../tests/codes/val/test_sst_val211.py),
[test_sst_cfg043.py](../../tests/codes/cfg/test_sst_cfg043.py).

**SST depends on it at**:
[val_relationship.py](../../snowflake_semantic_tools/domain/diagnostics/specs/val_relationship.py);
[keys.py](../../snowflake_semantic_tools/domain/model/config_schema/keys.py);
[the migration guide](../guides/migrating-from-0.3.md).

### A PRIVATE dimension works, although the docs deny it

**Docs say**: "You cannot mark a dimension as private. Dimensions are always
public."

**Server does**: it accepts a `PRIVATE` dimension, stores it, and enforces it:
a query that names it fails with `invalid identifier`, as for a private fact or
metric. The same query against a public dimension succeeds.

**Evidence**: `Enforced`, in the other direction. SST refuses a private dimension
(SST-VAL222), and the reason is now a house rule rather than the grammar. It
would depend on behaviour the vendor's docs deny, which could be removed without
notice. Do not loosen SST-VAL222 on the strength of the server.
[test_sst_val222.py](../../tests/codes/val/test_sst_val222.py).

**SST depends on it at**: `_column_meta_diagnostics` in
[validate/semantic/dbt.py](../../snowflake_semantic_tools/domain/validate/semantic/dbt.py).

### Replacing a view without WITH TAG removes its tags

**Docs say**: the reference documents `WITH TAG` as a clause of
`CREATE SEMANTIC VIEW`. It says nothing about what a replace does to tags set
earlier with `ALTER SEMANTIC VIEW ... SET TAG`.

**Server does**: `CREATE OR REPLACE SEMANTIC VIEW` without `WITH TAG` removes
every tag on the view, silently. With `WITH TAG` inline, the replace carries the
tags in one statement.

**Evidence**: `Observed`. The clause-order test above pins that SST renders tags
inline in the `CREATE`, and
`test_key_summaries_describe_what_the_renderer_and_the_stage_channel_do` in
[test_config_schema.py](../../tests/unit/domain/test_config_schema.py) holds the
`tags` key's summary to that. No test
proves that the replace drops tags set outside it. The consequence for users is
that every apply leaves a view with exactly the tags the project declares, and a
governance tag applied by hand is gone after the next apply. Plan reads the
tags on each view it will replace, once it has decided the update, and warns of
each one the project does not declare (`SST-PLN039`). A read that fails is a
warning, `SST-PLN040`, and the plan goes on. Probe:

```sql
-- Create probe_view, then:
ALTER SEMANTIC VIEW <db>.<schema>.probe_view SET TAG <db>.<schema>.probe_tag = 'x';
SELECT * FROM TABLE(<db>.INFORMATION_SCHEMA.TAG_REFERENCES(
  '<db>.<schema>.probe_view', 'TABLE'));
-- Expect: a row for probe_tag. If not, see the next entry.
-- Re-run the same CREATE OR REPLACE without WITH TAG, then the same SELECT.
-- Expect: no row for probe_tag.
```

**SST depends on it at**: `_trailing_clauses` in
[semantic_view.py](../../snowflake_semantic_tools/domain/render/semantic_view.py);
`observe_replaced_tags` in [observe.py](../../snowflake_semantic_tools/app/observe.py);
`_removed_tags` in [classify.py](../../snowflake_semantic_tools/domain/plan/classify.py);
the `tags:` key in [the semantic views guide](../guides/semantic-views.md).

### TAG_REFERENCES lists no domain for a semantic view

**Docs say**: the `TAG_REFERENCES` table function takes an object name and an
`object_domain`. The documented domains have no semantic-view entry. They say
`'TABLE'` is for all table-like objects, such as views, materialized views, and
external tables.

**Server does**: not probed. No recorded session shows `TAG_REFERENCES`
answering for a semantic view under any domain, so which domain lists its tags
is unknown. SST sends `'TABLE'`, as the docs direct for a table-like object.
An earlier probe sketch named `'SEMANTIC_VIEW'`, but nothing records that it ran.

**Evidence**: `Observed`. The connector test pins the statement SST sends, not
that the server answers it. If the domain is wrong, plan either reads no tags,
and `SST-PLN039` stays silent about a hand-applied tag a replace removes, or the
read fails and plan warns that it skipped the check (`SST-PLN040`). The release
probe must settle it:

```sql
-- With probe_view tagged as in the entry above:
SELECT * FROM TABLE(<db>.INFORMATION_SCHEMA.TAG_REFERENCES(
  '<db>.<schema>.probe_view', 'TABLE'));
SELECT * FROM TABLE(<db>.INFORMATION_SCHEMA.TAG_REFERENCES(
  '<db>.<schema>.probe_view', 'SEMANTIC_VIEW'));
-- Expect: the domain SST sends lists probe_tag. If only the other does, change
-- _SEMANTIC_VIEW_TAG_DOMAIN and its test.
```

**SST depends on it at**: `_SEMANTIC_VIEW_TAG_DOMAIN` and `tag_references` in
[catalog.py](../../snowflake_semantic_tools/adapters/snowflake/connector/catalog.py);
`test_tag_references_name_each_tag_set_on_a_view_and_leave_out_inherited_ones`
in [test_snowflake_connector.py](../../tests/unit/adapters/test_snowflake_connector.py).

## Column tags

### TAG_REFERENCES_ALL_COLUMNS lists inherited tags on every column

**Docs say**: not compared for this entry. What follows was measured, and SST
relies on the measurement, not on the documentation's description.

**Server does**: measured on a live account, on a table:

```sql
SELECT * FROM TABLE(<db>.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
  '<db>.<schema>.<table>', 'table'));
```

answers with the columns `TAG_DATABASE`, `TAG_SCHEMA`, `TAG_NAME`, `TAG_VALUE`,
`LEVEL`, `OBJECT_DATABASE`, `OBJECT_SCHEMA`, `OBJECT_NAME`, `DOMAIN`,
`COLUMN_NAME`, `APPLY_METHOD`. A tag set on one column is one row for that column,
with `LEVEL = COLUMN` and `APPLY_METHOD = MANUAL`. A tag set on the schema is one
row on every column of the table, with `LEVEL = SCHEMA` and
`APPLY_METHOD = INHERITED`. The same read under the account's `PUBLIC` role
answered the same rows. A table that does not exist fails the read with errno
2003, "does not exist or not authorized".

Not measured: what a role is answered for a tag it is not allowed to see (no row,
or an error); the read on a view rather than a table; and the `PROPAGATED` and
`CLASSIFIED` apply methods, which SST counts as it counts the others.

**Evidence**: `Enforced` for SST's side. `parse_column_tags` in
[governance.py](../../snowflake_semantic_tools/domain/enrich/governance.py) keeps
every row whatever its `LEVEL` and `APPLY_METHOD`, so a tag inherited from the
schema marks every column; `column_tags` in
[profiler.py](../../snowflake_semantic_tools/adapters/snowflake/connector/profiler.py)
lets a failed read escape, where `relation_columns` reads the same errno as "no
relation", so `sst enrich` fails the model (`SST-SNO031`) rather than sampling a
column it could not check. `test_every_level_and_apply_method_counts_and_each_tag_is_kept_once_per_column`
in [test_governance.py](../../tests/unit/domain/test_governance.py), and
`test_column_tags_keep_every_level_and_apply_method_by_casefolded_column` and
`test_a_tag_read_that_fails_or_answers_another_shape_raises` in
[test_enrich_port.py](../../tests/contract/test_enrich_port.py), pin it. The read
fails closed only on an error: if Snowflake answers a role that may not see a tag
with no row for it, the column reads as untagged, and only its YAML's `pii_tags`
or `meta.sst.governance_tags` keeps its values unread. Probe:

```sql
-- In a scratch schema, as a role that may set tags:
CREATE TAG <db>.<schema>.probe_tag;
CREATE TABLE <db>.<schema>.probe_t (a TEXT, b TEXT);
ALTER TABLE <db>.<schema>.probe_t MODIFY COLUMN a SET TAG <db>.<schema>.probe_tag = 'x';
ALTER SCHEMA <db>.<schema> SET TAG <db>.<schema>.probe_tag = 'y';
SELECT * FROM TABLE(<db>.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
  '<db>.<schema>.probe_t', 'table'));
-- Expect: a COLUMN/MANUAL row on A, and a SCHEMA/INHERITED row on A and on B.
-- Repeat as a role with SELECT on probe_t and no privilege on the tag, and record
-- whether it sees the rows, sees none, or fails; then over a view on probe_t.
```

**SST depends on it at**: `column_tags_sql` and `parse_column_tags` in
[governance.py](../../snowflake_semantic_tools/domain/enrich/governance.py);
`column_tags` in
[profiler.py](../../snowflake_semantic_tools/adapters/snowflake/connector/profiler.py);
`read_tags` in [validate_tags.py](../../snowflake_semantic_tools/app/validate_tags.py);
[Governance tags](../guides/enrich.md#governance-tags) in the enrich guide.

## Agents

### CREATE OR REPLACE AGENT destroys versions and aliases

**Docs say**: `COPY GRANTS` keeps an agent's grants across `CREATE OR REPLACE`.
The reference says nothing about its versions.

**Server does**: a replaced agent comes back with only `VERSION$1`. Every
committed version and every alias a caller pins is gone, and the statement
reports success.

**Evidence**: `Enforced`. SST never replaces a permanent agent: a new agent is
`CREATE AGENT`, and an update adds a version.
`test_permanent_agent_create_leaves_one_committed_version_then_alias_and_tags`
and `test_permanent_agent_update_adds_exactly_one_version_then_alias_and_tags` in
[test_agent_publish.py](../../tests/unit/app/test_agent_publish.py) pin the
exact statements. `CREATE OR REPLACE TEMPORARY AGENT`, for `--temporary`, is a
different object and is not affected.

**SST depends on it at**: `_agent_programs` in
[compiled.py](../../snowflake_semantic_tools/app/compile/agents/compiled.py); the
agent case in [prior.py](../../snowflake_semantic_tools/app/prior.py), which
captures a version rather than `GET_DDL`; the restore advice in
[the CI/CD guide](../guides/ci-cd.md#recording-what-an-update-replaces).

### ADD VERSION fails beside a live version, and COMMIT is not idempotent

**Docs say**: `CREATE AGENT` commits `VERSION$1` and also opens a live version
with the same spec. `ADD VERSION FROM @<stage>/<path>` creates a named version.
The docs do not say that the second fails while the first is open.

**Server does**: `ADD VERSION` beside a live version fails:
`There is already a live version. Please commit it first.` `COMMIT` with nothing
live fails: `Live version is not found.` So a deployer cannot emit `COMMIT`
unconditionally, and must read before it writes. A fresh agent's `COMMIT` is
always `VERSION$2`. The live version shows only in `SHOW VERSIONS IN AGENT`, as
the row whose `name` is null; `DESCRIBE AGENT`'s version list omits it.

An agent needs its live version all the same. Snowsight's agent page shows the
live version (as DRAFT), and `DESCRIBE AGENT`'s `agent_spec` is the live
version's spec, empty when there is none. A request that names no version is
served the live version while the agent has one committed version. Measured on
an agent with only `VERSION$1` and no live version: Snowsight showed no
instructions, tools or skills; an unversioned `DATA_AGENT_RUN` ran without the
agent's tools or skills; the same request naming `!VERSION$1` or `!DEFAULT` was
served the full spec. `ALTER AGENT ... ADD LIVE VERSION FROM LAST` opens a live
version holding `LAST`'s spec. `LAST` is the only source it takes: `FROM
VERSION$1`, `FROM "VERSION$1"` and `FROM VERSION VERSION$1` are syntax errors
(001003), so a live version can be reopened only from the newest committed
version. `SET DEFAULT_VERSION` takes the version, or a shortcut, only as a
string literal (`'VERSION$3'`, `'LAST'`): an identifier is a syntax error, and
so is an unquoted `LAST`, although Snowflake's versioning guide shows one. A
default set to `'LAST'` follows every version committed afterwards: measured, an
agent whose default was reset to `'LAST'` served each new version as its default
without another statement, while one whose default was set to `'VERSION$3'`
kept serving `VERSION$3` after `VERSION$4` was committed. `SHOW VERSIONS IN
AGENT` marks only which version is the default (`is_default`), not which of the
two was set, so a default pinned by name to the newest version reads the same as
one that follows `LAST`.

**Evidence**: `Enforced`. On create, SST keeps the live copy `CREATE AGENT`
opens. On update, an agent with a live version gets the spec written into it
and committed, instead of an `ADD VERSION`; one without gets an `ADD VERSION`.
Either way the agent has no live version after that, so `ADD LIVE VERSION FROM
LAST` opens one holding what was just committed. Every version-adding update,
and every repair of a moved default, sets the default to `'LAST'`, never to a
version by name, so the default keeps following what SST commits.
[test_agent_publish.py](../../tests/unit/app/test_agent_publish.py)
(`test_permanent_agent_create_keeps_the_live_version_then_alias_and_tags`,
`test_permanent_agent_update_adds_exactly_one_version_and_a_live_copy_then_alias_and_tags`,
and
`test_permanent_agent_update_writes_a_live_version_and_commits_it_as_the_one_new_version`);
`test_agent_versions_report_the_live_row_and_every_shown_alias` in
[test_snowflake_connector.py](../../tests/unit/adapters/test_snowflake_connector.py).

**SST depends on it at**: `_agent_programs`
in [compiled.py](../../snowflake_semantic_tools/app/compile/agents/compiled.py);
`observe_agent_versions` in
[catalog.py](../../snowflake_semantic_tools/adapters/snowflake/connector/catalog.py);
`AgentVersions.has_live_version` in
[ports/snowflake/catalog.py](../../snowflake_semantic_tools/domain/ports/snowflake/catalog.py);
[the agents guide](../guides/agents.md#publishing).

### LAST must be quoted, and an alias cannot be a reserved word

**Docs say**: `LIVE`, `FIRST`, `LAST` and `DEFAULT` are version shortcuts "in
the API endpoint and in stage URIs". The examples of `MODIFY VERSION` name
`VERSION$<n>`.

**Server does**: `MODIFY VERSION LAST SET ALIAS = <alias>` is a syntax error.
`MODIFY VERSION "LAST"` and `MODIFY VERSION VERSION$<n>` are accepted; bare
`DEFAULT` and `FIRST` are refused too. An alias that is a reserved word, such as
`current`, is a syntax error unquoted. Quoted, it is stored case-sensitively, and
every later reference must quote it too. A version carries one alias, and
setting another replaces it silently.

**Evidence**: `Enforced`. SST renders `MODIFY VERSION "LAST"`, and refuses a
shortcut or a reserved word as an alias (SST-PRS025) rather than quote it.
[test_agent_publish.py](../../tests/unit/app/test_agent_publish.py),
[test_agent_alias_names.py](../../tests/unit/security/test_agent_alias_names.py),
[test_sst_prs025.py](../../tests/codes/prs/test_sst_prs025.py).

**SST depends on it at**: `_agent_programs` and `_alias` in
[compiled.py](../../snowflake_semantic_tools/app/compile/agents/compiled.py);
`RESERVED_AGENT_ALIASES` in
[model/agent.py](../../snowflake_semantic_tools/domain/model/agent.py);
`reserved_alias` in
[validate/agent.py](../../snowflake_semantic_tools/domain/validate/agent.py).

### A staged spec must be agent_spec.yaml, and may hold JSON

**Docs say**: `agent_spec.yaml` appears only when reading a version back
(`GET snow://agent/<name>/versions/<version>/agent_spec.yaml`). The docs show
`CREATE AGENT ... FROM @<stage>/<path>` and `ADD VERSION FROM` a stage
directory without naming the file they read.

**Server does**: it reads the spec from `agent_spec.yaml` in that directory. A
JSON document in that file is accepted.

**Evidence**: `Enforced`. `test_permanent_agent_create_leaves_one_committed_version_then_alias_and_tags`
in [test_agent_publish.py](../../tests/unit/app/test_agent_publish.py) asserts
the upload path, and the agent goldens are JSON
([test_golden_agents.py](../../tests/unit/test_golden_agents.py)).

**SST depends on it at**: the `Upload` in `CompiledAgent.rendered_artifact` in
[compiled.py](../../snowflake_semantic_tools/app/compile/agents/compiled.py).

### A profile over 1,000 bytes is refused, and only once the agent exists

**Docs say**: `ALTER AGENT ... SET PROFILE` takes a JSON object of
`display_name`, `avatar`, and `color`. The docs give no size limit.

**Server does**: a profile over 1,000 bytes of JSON is refused:
`Cortex agent profile JSON exceeds maximum size limit of 1000 bytes. Actual
size: <n> bytes.` `CREATE AGENT` and every version statement before it have
already run, so the publish stops part way.

**Evidence**: `Enforced`. Validation refuses such a profile (SST-VAL558), and
publishing writes the marked comment before anything else, so a publish that
stops part way is finished by the next apply.
[test_sst_val558.py](../../tests/codes/val/test_sst_val558.py),
`test_a_set_profile_refused_after_the_create_reports_snowflakes_error_and_a_partial_write`
in [test_apply.py](../../tests/unit/app/test_apply.py).

**SST depends on it at**: `PROFILE_LIMIT_BYTES` in
[validate/agent.py](../../snowflake_semantic_tools/domain/validate/agent.py).

### An mcp tool is accepted, and every run of the agent fails

**Docs say**: an MCP server is attached to an agent with the spec's top-level
`mcp_servers:` key, `mcp_servers: [{server_spec: {name: <server>}}]`.

**Server does**: `CREATE AGENT` accepts a `tools` entry of `type: mcp`, as it
accepts any tool type, and then every run of the agent fails before
orchestration with `Tool type mcp is not valid` (399504), whatever the question.
The same server under `mcp_servers:` is called, its tools named
`<server>_<tool>`.

**Evidence**: `Enforced`. SST refuses `type: mcp` (SST-VAL559);
`spec.passthrough` renders `mcp_servers:`.
[test_sst_val559.py](../../tests/codes/val/test_sst_val559.py).

**SST depends on it at**: `resolve_tool` in
[resolve_tools.py](../../snowflake_semantic_tools/app/compile/agents/resolve_tools.py).

### Agent references are not checked by the server

**Docs say**: the `CREATE AGENT` reference does not describe a skills block, so
says nothing about whether a skill reference is checked.

**Server does**: it accepts a skill whose extension, version alias, or skill
name does not exist, and a version that is empty. `DESCRIBE`, `SHOW` and
`COMMIT` all pass on a dangling reference, so nothing at publish time reports it.
A `generic` tool's `tool_resources` are not checked either.

**Evidence**: `Enforced`. For what SST publishes, publish order is the only
control. Skills and plugins publish before agents (`ddl_position` 250 and 260,
before 300, in
[registry.py](../../snowflake_semantic_tools/domain/model/registry.py)), and a
plan that publishes an agent must include every skill it pins (SST-PLN030). An
extension another project publishes is looked up instead: a connected `sst plan`
reads it and the version the agent pins, even with the connected checks off, and
one that is absent or hidden is an error (SST-VAL531). A skill name inside a
consumed extension is still not checked. SST-RND013 warns on rendered generic
tool resources.
[test_sst_pln030.py](../../tests/codes/pln/test_sst_pln030.py),
[test_sst_val531.py](../../tests/codes/val/test_sst_val531.py),
[test_sst_rnd013.py](../../tests/codes/rnd/test_sst_rnd013.py).

**SST depends on it at**:
[plan/prune.py](../../snowflake_semantic_tools/domain/plan/prune.py);
`references` in
[agents/observe.py](../../snowflake_semantic_tools/app/compile/agents/observe.py);
[the agents guide](../guides/agents.md#skills).

### The orchestration model is checked only when the agent runs

**Docs say**: `models.orchestration` names the orchestration model, for example
`auto`, which lets Snowflake choose. The Cortex Agents page lists the supported
models by cross-region scope.

**Server does**: `CREATE AGENT` accepts any string there, including an embedding
model, a retired model and a name that is no model at all, and `DESCRIBE AGENT`
returns it unchanged. A bad model fails only when the agent runs:
`DATA_AGENT_RUN` returns code `399504`, with "<model> is not an allowed model
for Agent requests" for a real model agents never accept (`llama3.1-8b`), and
"<model> is not authorized or not available in your region" for one the account
cannot reach. Each message lists the models available under a "Diagnostics"
heading, in a format no document fixes. `SHOW CORTEX BASE MODELS` lists every
Cortex model with its `lifecycle_status` (`GA`, `PUPR`, `PRPR`, `LEGACY`, `EOL`,
`INTERNAL`) and marks none as agent-capable, so it cannot stand in for a run.
Neither matches the docs' table: an account can be offered models the table
omits, some of them `INTERNAL` or absent from SHOW, and not offered models it
lists. ACCOUNTADMIN reaches every model, so a run as it proves nothing for
other roles. `AI_COMPLETE` refuses an embedding model, a retired model and a
misspelt name alike, with `400 'unknown model "<model>"'`, and a legacy model
an account never used with "has been in legacy state" (`mistral-large2`, SST's
former synonym default, from October 2026); `EXPLAIN` accepts all of
them, so only a call shows a model answers.

**Evidence**: `Enforced`. Offline, `snowflake.orchestration_models`, default
`[auto]`, is the guard, and SST-VAL543 refuses a model outside it. Connected,
each model in it, `auto` aside, runs once in a TEMPORARY probe agent with no
tools: SST-VAL551 when the run refuses it, SST-VAL552 to SST-VAL555 for its
lifecycle, SST-VAL556 when nothing could be learned. Observed on an AWS US
account in October 2026.
[test_sst_val543.py](../../tests/codes/val/test_sst_val543.py),
[test_sst_val551.py](../../tests/codes/val/test_sst_val551.py),
[test_cortex_models_port.py](../../tests/contract/test_cortex_models_port.py).

**SST depends on it at**:
[val_agent.py](../../snowflake_semantic_tools/domain/diagnostics/specs/val_agent.py);
[validate_models.py](../../snowflake_semantic_tools/app/validate_models.py);
[cortex_models.py](../../snowflake_semantic_tools/adapters/snowflake/connector/cortex_models.py);
[the configuration reference](../reference/config.md).

## Cortex extensions and stages

### The extension grammar is undocumented

**Docs say**: nothing. The published SQL reference has no
`CREATE CORTEX EXTENSION` or `ALTER CORTEX EXTENSION` page, and `GET_DDL` reads
no extension. Every behaviour in this section comes from probing.

**Server does**: `CREATE CORTEX EXTENSION ... FROM <location> VERSION <alias>`
must put `FROM` and `VERSION` before `TYPE`. The name given to `ADD VERSION`
becomes the version's alias. The version itself is `VERSION$<n>`, and its files
list only under `versions/version$<n>/`: the alias path lists nothing.

**Evidence**: `Enforced`. `test_first_publish_then_noop_then_revert_is_a_state_only_update`
in [test_extension_lifecycle.py](../../tests/unit/app/test_extension_lifecycle.py)
pins the exact `CREATE`. `test_list_location_returns_paths_relative_to_a_prefix_or_version`
in [test_snowflake_connector.py](../../tests/unit/adapters/test_snowflake_connector.py)
pins version listing by system name.

**SST depends on it at**: `_create_sql` and `_version` in
[extensions.py](../../snowflake_semantic_tools/app/lifecycle/extensions.py);
`_EXTENSION_URI` in
[stage.py](../../snowflake_semantic_tools/adapters/snowflake/connector/stage.py).

### An extension created without a source starts with an empty version

**Docs say**: nothing (see above).

**Server does**: `CREATE CORTEX EXTENSION` without `FROM ... VERSION` leaves an
empty, unaliased `VERSION$1`. No statement can drop or alias it, and the catalog
serves it until a version is added.

**Evidence**: `Enforced`. SST creates a skill's extension from its bundle, so the
first version is the bundle; the create test above asserts no `ADD VERSION`
follows. Only a plugin has a fallback: when the bundled create is refused it is
created bare and a version added, and when that is refused too a live version is
built from empty:
`test_plugin_falls_back_to_a_live_version_built_from_empty`.

**SST depends on it at**: `_create_extension` in
[extensions.py](../../snowflake_semantic_tools/app/lifecycle/extensions.py);
step 3 of publishing in [the skills guide](../guides/skills.md#publishing).

### ADD VERSION from a path that holds nothing succeeds

**Docs say**: nothing (see above).

**Server does**: `ADD VERSION <alias> FROM @<stage>/<path>/` succeeds when the
path matches no file, reports `Version <alias>successfully created.`, and
produces an empty version. An agent that pins it then behaves as though the
skill had no content, with no error anywhere.

**Evidence**: `Enforced`. After every create or add, SST lists the version and
requires exactly the bundle's files (SST-APL016, SST-APL019).
`test_empty_version_and_upload_failures_are_partial_writes` in
[test_extension_lifecycle.py](../../tests/unit/app/test_extension_lifecycle.py)
reproduces the empty version;
[test_sst_apl016.py](../../tests/codes/apl/test_sst_apl016.py).

**SST depends on it at**: `_check_version` in
[extensions.py](../../snowflake_semantic_tools/app/lifecycle/extensions.py).

### A skill version holds skills/ with exactly one skill

**Docs say**: nothing (see above).

**Server does**: for `TYPE = 'SKILL'`, a version root must contain only a
`skills/` directory, and `skills/` must contain exactly one subdirectory. Both
are enforced only by error message. A version whose `SKILL.md` sits at the root
is rejected.

**Evidence**: `Enforced`. Each skill is its own extension, and its bundle puts
every file under `skills/<name>/`. The skill goldens in
[tests/golden/expected/skill](../../tests/golden/expected/skill) pin that layout,
and [test_goldens.py](../../tests/unit/test_goldens.py) compares them
(`test_every_compiled_payload_equals_its_golden`).

**SST depends on it at**:
[skill_bundle.py](../../snowflake_semantic_tools/domain/render/skill_bundle.py);
[the skills guide](../guides/skills.md).

### LIST names a stage file with the stage's own name first

**Docs say**: `LIST`'s `name` column is the "Name of the staged file".

**Server does**: on an internal stage, `name` starts with the stage's own name,
unqualified and lower-cased, so it is not a path into the stage. Fed back into
`ADD VERSION ... FROM`, it names a path that holds nothing, which the previous
entry shows succeeds silently.

**Evidence**: `Enforced`. SST compares names after the first `/`, and accepts
the stage-name prefix only for the exact path it asked for.
`test_list_location_returns_paths_relative_to_a_prefix_or_version` and
`test_staged_file_observation_accepts_internal_stage_name_shape_only_for_exact_path`
in [test_snowflake_connector.py](../../tests/unit/adapters/test_snowflake_connector.py).

**SST depends on it at**: `list_location` and `_staged_file_name_matches` in
[stage.py](../../snowflake_semantic_tools/adapters/snowflake/connector/stage.py).

## Evaluations

### EXECUTE_AI_EVALUATION needs a current database and schema

**Docs say**: "With SQL, agent evaluations are run on the session's database and
schema." Here the docs agree with the server. The entry exists because of the
error.

**Server does**: called unqualified in a session with no current database, the
`START` fails with `Cannot perform DROP. This session does not have a current
database.` That names an operation the caller never asked for, although every
argument is fully qualified. A session that inherited a schema from earlier
statements succeeds by accident, so the failure shows first in a fresh session,
as in CI.

**Evidence**: `Enforced`. SST runs `START` and `STATUS` on a session opened with
the agent's database and schema as current, and checks them on connect.
`test_eval_runner_polls_retrieves_scores_and_costs` in
[test_eval_run.py](../../tests/unit/app/test_eval_run.py) asserts the `START`
runs in the agent's schema.

**SST depends on it at**: `_start` in
[run.py](../../snowflake_semantic_tools/app/evals/run.py); `read_status` in
[retrieve.py](../../snowflake_semantic_tools/app/evals/retrieve.py);
`_open_scoped` in
[session.py](../../snowflake_semantic_tools/adapters/snowflake/connector/session.py).

### A system metric cannot name version v1

**Docs say**: every system metric accepts a `version` key, and the versions
table lists `v1`. Omitting `version`, or `auto`, uses the current default.

**Server does**: `version: v1` is refused at `START`, with a message telling the
caller to omit the version field to use v1. `v2` and `v3` can be named. So v1 is
reachable only as an unpinned default.

**Evidence**: `Enforced` for the pin: SST requires every system metric to name a
version (SST-VAL722), because an unpinned default moves when Snowflake changes
it. [test_sst_val722.py](../../tests/codes/val/test_sst_val722.py). The refusal
of `v1` is `Observed`. SST accepts only `v3` (SST-PRS013, SST-VAL723), and
refuses `v1` with a code no setting lowers (SST-VAL764), so a project that
demotes SST-PRS013 still never sends it:
[test_sst_prs013.py](../../tests/codes/prs/test_sst_prs013.py),
[test_sst_val723.py](../../tests/codes/val/test_sst_val723.py),
[test_sst_val764.py](../../tests/codes/val/test_sst_val764.py). Probe:

```sql
-- Stage an evaluation config for <db>.<schema>.probe_agent whose one system
-- metric sets `version: v1`, at @<db>.<schema>.probe_stage/probe.yaml, then:
USE SCHEMA <db>.<schema>;
CALL EXECUTE_AI_EVALUATION('START', OBJECT_CONSTRUCT('run_name', 'probe_run'),
  '@<db>.<schema>.probe_stage/probe.yaml');
-- Expect: refused, telling the caller to omit the version to use v1.
-- Set `version: v3` and the same START is accepted.
```

**SST depends on it at**: `_system_metric` in
[eval_config.py](../../snowflake_semantic_tools/domain/validate/eval_config.py);
`_system_metric_value` in
[render/eval.py](../../snowflake_semantic_tools/domain/render/eval.py);
[the evals guide](../guides/evals.md#the-run-configuration).

### PARTIALLY_COMPLETED is reported before COMPLETED

**Docs say**: nothing found about a run passing through a partial status on its
way to completing.

**Server does**: between a run's last metric and its finalizing steps, `STATUS`
can read `PARTIALLY_COMPLETED` for up to about a minute, after which the same run
reads `COMPLETED`.

**Evidence**: `Enforced`. A partial status ends a run only once it has persisted
for `partial_settle_ms`, three minutes by default.
`test_a_partial_status_that_turns_completed_within_the_window_is_kept_without_a_retry`
in [test_eval_poll.py](../../tests/unit/app/test_eval_poll.py) covers the window.

**SST depends on it at**: the module docstring and polling loop of
[poll.py](../../snowflake_semantic_tools/app/evals/poll.py);
`partial_settle_ms` in
[options.py](../../snowflake_semantic_tools/app/evals/options.py).

### An evaluation ignores secondary roles

**Docs say**: the evaluation page lists the privileges a run needs and does not
mention secondary roles.

**Server does**: a run executes as Snowflake tasks under the session's primary
role only. A privilege the session holds through a secondary role works
interactively and fails in the run.

**Evidence**: `Enforced`. Before any run starts, SST-VAL727 reports a run
privilege held only through a secondary role, and SST-VAL728 one held by no role.
[test_sst_val727.py](../../tests/codes/val/test_sst_val727.py),
[test_sst_val728.py](../../tests/codes/val/test_sst_val728.py).

**SST depends on it at**:
[privileges.py](../../snowflake_semantic_tools/app/evals/privileges.py);
[the security guide](../guides/security.md).

## Search services and grants

### A search service stays ACTIVE after its source is rebuilt

**Docs say**: change tracking must be enabled on every object a Cortex Search
service reads, and a service that cannot refresh incrementally fails at
creation.

**Server does**: after the source table is replaced, including by an ordinary
`dbt build` that rematerialises it, `ALTER CORTEX SEARCH SERVICE ... REFRESH`
fails. The service still reports `ACTIVE` indexing and serving states and a
plausible row count. Only replacing the service recovers it. Its status fields
cannot show the breakage, so a monitor that reads them sees a healthy service.

**Evidence**: `Observed`. SST never reads the service's status to judge health.
It warns when a dbt materialization rebuilds the relation an `INCREMENTAL`
service indexes (SST-VAL617), and when connected validation finds the source's
change tracking off (SST-VAL618).
[test_sst_val617.py](../../tests/codes/val/test_sst_val617.py),
[test_sst_val618.py](../../tests/codes/val/test_sst_val618.py). No test stops a
change that trusts the status instead. Probe:

```sql
CREATE TABLE <db>.<schema>.probe_docs (ID INT, BODY STRING) CHANGE_TRACKING = TRUE;
INSERT INTO <db>.<schema>.probe_docs VALUES (1, 'a'), (2, 'b');
CREATE CORTEX SEARCH SERVICE <db>.<schema>.probe_search ON BODY
  WAREHOUSE = <warehouse> TARGET_LAG = '1 hour'
  AS SELECT ID, BODY FROM <db>.<schema>.probe_docs;
CREATE OR REPLACE TABLE <db>.<schema>.probe_docs (ID INT, BODY STRING);
ALTER CORTEX SEARCH SERVICE <db>.<schema>.probe_search REFRESH;
SHOW CORTEX SEARCH SERVICES LIKE 'PROBE_SEARCH' IN SCHEMA <db>.<schema>;
-- Expect: the REFRESH fails while SHOW still reports ACTIVE.
```

**SST depends on it at**: `rebuilt_source` in
[validate/tool.py](../../snowflake_semantic_tools/domain/validate/tool.py);
`_tool` and `_change_tracking` in
[agents/observe.py](../../snowflake_semantic_tools/app/compile/agents/observe.py);
[val_tool.py](../../snowflake_semantic_tools/domain/diagnostics/specs/val_tool.py).

### Which replaces can keep their grants

**Docs say**: `CREATE AGENT` has `COPY GRANTS`, and it does not copy future
grants. `COPY GRANTS` and `IF NOT EXISTS` are mutually exclusive on it.
`CREATE CORTEX SEARCH SERVICE` has no `COPY GRANTS` clause. `SHOW GRANTS ON
<object_type> <object_name>` is given for any object.

**Server does**: it agrees on agents and search services. A replace without the
clause drops every explicit grant, for an agent, a dataset and an extension
alike. `COPY GRANTS` is a syntax error on `CREATE DATASET` and on
`CREATE CORTEX EXTENSION`. `SHOW GRANTS ON DATASET <name>` is a syntax error in
every form, so a dataset's grants are visible only from the grantee's side
(`SHOW GRANTS TO ROLE`), and its owner from `SHOW DATASETS`.

**Evidence**: `Enforced`. Each rendered object's `PublishShape` declares how its
grants survive an update. A semantic view, procedure and function replace with
`COPY GRANTS`. A search service is replaced and its explicit grants re-applied.
Agents, skills, plugins, profiles and evals are never replaced. SST never sends
`SHOW GRANTS ON DATASET`.
`test_search_service_update_replays_and_verifies_explicit_grants` in
[test_apply.py](../../tests/unit/app/test_apply.py);
`test_sst_val713_silent` in
[test_sst_val713.py](../../tests/codes/val/test_sst_val713.py);
`test_replacing_the_view_keeps_the_grants_on_it`, live, in
[test_apply_live.py](../../tests/integration/test_apply_live.py).

**SST depends on it at**: `GrantPreservation` and each type's
`grant_preservation` in
[registry.py](../../snowflake_semantic_tools/domain/model/registry.py); the
`PublishShape` a search service, procedure or function renders with, in
[render/tool.py](../../snowflake_semantic_tools/domain/render/tool.py);
`_verify_grants` in [apply/one.py](../../snowflake_semantic_tools/app/apply/one.py);
`_UNLISTED_GRANTS` in
[catalog.py](../../snowflake_semantic_tools/adapters/snowflake/connector/catalog.py).

## Roles and privileges

### CREATE is authorised by the primary role alone

**Docs say**: a session's secondary roles authorise any SQL action except
`CREATE <object>`, which is authorised by the primary role alone.
`IS_ROLE_IN_SESSION` reports a role active through the primary role or a
secondary role.

**Server does**: not probed. SST takes the documented rule as stated.

**Evidence**: `Enforced`. The rule is documented, not observed, and SST acts
on it in two places. `SST-PLN008` counts a `CREATE` privilege only when the
primary role holds it through its hierarchy, and asks `IS_ROLE_IN_SESSION` only
to say that a secondary role holds one it reports; `READ` and `WRITE` on a spec
stage still count for any role in the session
([test_preflight.py](../../tests/unit/app/test_preflight.py)). The eval check
acts on the same rule for its task privileges; see
[An evaluation ignores secondary roles](#an-evaluation-ignores-secondary-roles).
Probe:

```sql
-- With CREATE TABLE on <db>.<schema> granted only to <secondary_role>:
USE ROLE <primary_role>;
USE SECONDARY ROLES <secondary_role>;
SELECT IS_ROLE_IN_SESSION('<secondary_role>');
-- Expect: TRUE.
CREATE TABLE <db>.<schema>.probe_table (id NUMBER);
-- Expect: refused for insufficient privileges, if the documented rule holds.
USE SECONDARY ROLES NONE;
```

**SST depends on it at**: `missing_privileges` and `missing_role_privileges` in
[ports/snowflake/preflight.py](../../snowflake_semantic_tools/domain/ports/snowflake/preflight.py);
`_lacking` and `missing_role_privileges` in
[connector/preflight.py](../../snowflake_semantic_tools/adapters/snowflake/connector/preflight.py);
`_missing_privileges` in
[app/preflight.py](../../snowflake_semantic_tools/app/preflight.py), which splits
what the primary role lacks into what a secondary role holds, what the session
holds past the walk's depth limit, and what nothing does, and which asks the
session alone when `CURRENT_SECONDARY_ROLES()` names no secondary role; [the troubleshooting guide](../guides/troubleshooting.md#how-sst-reports-a-missing-privilege).

## Procedures and functions

### A JavaScript routine takes no RUNTIME_VERSION or HANDLER

**Docs say**: the `CREATE PROCEDURE` and `CREATE FUNCTION` grammars for Java,
Python and Scala require `RUNTIME_VERSION` and `HANDLER`. The JavaScript and SQL
grammars have neither clause: the body is the routine.

**Server does**: not probed. SST follows the documented grammar. The entry
exists because a reader who sees both clauses required for every other
non-SQL language could extend the requirement to JavaScript.

**Evidence**: `Enforced` for SST's side; the grammar itself is documented, not
observed. SST-PRS002 requires both fields only for Java, Python and Scala, and
the renderer emits both clauses only for those languages.
`test_a_javascript_routine_needs_no_runtime_version_or_handler_as_its_grammar_has_neither`
and `test_a_java_python_or_scala_routine_still_needs_runtime_version_and_handler`
in [test_tools.py](../../tests/unit/domain/test_tools.py);
`test_a_javascript_routine_renders_without_runtime_version_or_handler` in
[test_render_tools.py](../../tests/unit/domain/test_render_tools.py). Probe:

```sql
-- Acceptance: run both against a database that does not exist.
CREATE OR REPLACE FUNCTION <missing_db>.<schema>.probe_js() RETURNS FLOAT
  LANGUAGE JAVASCRIPT AS $$ return 1; $$;
-- Expect: does not exist or not authorized (the statement parsed).
CREATE OR REPLACE FUNCTION <missing_db>.<schema>.probe_js() RETURNS FLOAT
  LANGUAGE JAVASCRIPT RUNTIME_VERSION = '1' HANDLER = 'run' AS $$ return 1; $$;
-- Expect: a syntax error, if the documented grammar holds.
```

**SST depends on it at**: `HANDLER_LANGUAGES` in
[model/tool.py](../../snowflake_semantic_tools/domain/model/tool.py);
`_render_fields` in
[validate/tool.py](../../snowflake_semantic_tools/domain/validate/tool.py);
`_render_routine` in
[render/tool.py](../../snowflake_semantic_tools/domain/render/tool.py);
[the agents guide](../guides/agents.md).

### COMMENT goes before EXECUTE AS, and a routine with no arguments takes `()`

**Docs say**: the `CREATE PROCEDURE` grammar lists `COMMENT` before
`EXECUTE AS`; `ALTER PROCEDURE` and `ALTER FUNCTION` address a routine by its
name and argument types.

**Server does**: `... LANGUAGE SQL COMMENT = '..' EXECUTE AS CALLER AS $$..$$`
creates the procedure, and `COMMENT` after `EXECUTE AS` is a syntax error.
`CREATE FUNCTION ... RETURNS INT COMMENT = '..' AS '1'` works. `ALTER PROCEDURE
DB.S.P SET COMMENT` for a procedure with no arguments is a syntax error at `SET`;
`ALTER PROCEDURE DB.S.P() SET COMMENT` works.

**Evidence**: `Enforced`. `test_a_routine_renders_its_description_as_a_comment_before_execute_as`
and `test_a_function_with_no_arguments_is_addressed_with_empty_parentheses` in
[test_tool_compile.py](../../tests/unit/app/test_tool_compile.py).

**SST depends on it at**: `_render_routine` and `routine_signature` in
[render/tool.py](../../snowflake_semantic_tools/domain/render/tool.py);
`routine_ref` in [sql/names.py](../../snowflake_semantic_tools/domain/sql/names.py);
`rendered_for_publish` in [compile/tools.py](../../snowflake_semantic_tools/app/compile/tools.py).

### DESCRIBE PROCEDURE and GET_DDL read the same until the object changes

**Docs say**: nothing about whether the output is stable across reads.

**Server does**: `DESCRIBE PROCEDURE` answers one row per property (signature,
returns, language, `execute as`, body) and `GET_DDL('PROCEDURE', ..)` the same
text on every read; neither changes after `ALTER ... SET COMMENT`, and
`ALTER PROCEDURE ... EXECUTE AS OWNER` flips DESCRIBE's `execute as`. `SHOW
FUNCTIONS` and `SHOW PROCEDURES` list `is_secure` (`Y` or `N`), and `SHOW CORTEX
SEARCH SERVICES` lists `target_lag` (such as `1 day`) and `warehouse`.

**Evidence**: `Observed`. Plan's drift digest compares these reads with the ones
apply made after its write, so output that changed between Snowflake releases
would block every such object (`SST-PLN014`) until the next apply records it
again; `plan.drift_check: basic` skips the DESCRIBE and GET_DDL reads.
`test_a_procedure_s_drift_digest_is_stable_across_reads_and_a_comment_but_not_execute_as`
in [test_connector_contract.py](../../tests/contract_live/test_connector_contract.py)
is the probe. `SHOW VERSIONS IN DATASET` lists each version's METADATA in a
`metadata` column, as JSON text, beside the `SYSTEM_AI_OBS_...` version Snowflake
adds itself; `dataset_version_metadata` reads it (observed on a live account).

**SST depends on it at**: [drift.py](../../snowflake_semantic_tools/app/drift.py);
[model/lifecycle/drift.py](../../snowflake_semantic_tools/domain/model/lifecycle/drift.py);
`describe_routine`, `dataset_version_metadata` and `show_objects` in
[connector/catalog.py](../../snowflake_semantic_tools/adapters/snowflake/connector/catalog.py);
[concepts](../concepts.md#how-plan-decides-each-change).

## State and lock tables

### A transient schema's refusal of a hybrid table has a number; the account's does not

**Docs say**: not consulted for this entry, which records only what SST
measured; check Snowflake's hybrid table limitations when re-checking it.

**Server does**: `CREATE HYBRID TABLE` in a transient database or schema fails
with error 391412, "Cannot create hybrid table within transient database or
schema." (measured on a live account). The error of an account that cannot
create hybrid tables at all is not measured: the only account tested supports
them.

**Evidence**: `Enforced` for the transient schema: `create_refusal` reports
391412 as `SST-APL030`
([test_sst_apl030.py](../../tests/codes/apl/test_sst_apl030.py),
[test_state_convert.py](../../tests/unit/adapters/test_state_convert.py)).
`Observed` for the account: with no number to match, `create_refusal` reports
any other failed `CREATE` of a hybrid state or lock table whose message names
hybrid tables as `SST-APL035`
([test_sst_apl035.py](../../tests/codes/apl/test_sst_apl035.py)), so a wording
that does not name them is reported as Snowflake gave it. Probe, on an account
without hybrid tables:

```sql
CREATE HYBRID TABLE <db>.<schema>.probe_table (id NUMBER PRIMARY KEY);
-- Record the error number and message; then give create_refusal the number.
```

**SST depends on it at**: `create_refusal` in
[connector/lock_table.py](../../snowflake_semantic_tools/adapters/snowflake/connector/lock_table.py),
which the state and lock table creates and `sst state convert`
([connector/convert.py](../../snowflake_semantic_tools/adapters/snowflake/connector/convert.py))
share; [concepts](../concepts.md).

## Re-checking a behaviour

A behaviour here can change in any Snowflake release, with or without a change
to the docs. Re-check an entry when Snowflake ships a release that touches the
object, before relying on it in a new feature, or when a user reports something
that contradicts it. A check has three legs:

1. **Read the docs.** Find the current grammar and the claim the entry quotes.
   The docs are the cheapest source of a statement to try, and they may have
   caught up.
2. **Read SST's implementation.** Follow the entry's links to what SST emits or
   reads today, and the test that holds it.
3. **Probe the server.** Run the statement in a disposable scratch schema, record
   what happens, and drop what you created. Run the documented form and the form
   SST uses side by side, so one result is the other's control.

Probes come in three kinds, and the kind must match the claim:

- **Acceptance**: does the server accept or reject a construct? A syntax
  question needs only a statement the parser reaches. Run it against a database
  that does not exist: syntax is checked before names resolve, so a
  `does not exist or not authorized` error means the clause parsed. Pair it with
  the same statement minus the clause, which must fail the same way.
- **Round trip**: does an accepted construct survive read-back? Read it with
  `GET_DDL`, `DESCRIBE`, and for a semantic view
  `SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW`. They do not agree with each other on
  order, case, or injected defaults, so compare by meaning, never as raw text.
- **Execution**: does the created object work? Query it. `CREATE` succeeding is
  not evidence: the derived-metric and distinct-range entries both pass `CREATE`
  and fail, or give wrong numbers, only at query time.

A probe whose own artifacts could look like the vendor's behaviour proves
nothing. Check that a failure is not a stray terminator, a padding character that
appears in a keyword, or a pattern that matches part of a correct value.

**Every surprise found in the field becomes an entry here and a test.** The entry
records the docs, the server and the code site. The test pins SST's side, so the
next maintainer cannot follow the docs back into the defect without a failing
build. If the dependency cannot be pinned offline, the entry is `Observed` and
carries its probe.

The live suite does not run these probes. It has three layers, which
[Testing SST](../guides/testing-live.md#live-tests) describes with how to
configure an account and the scratch schemas they write to:

- [tests/contract_live](../../tests/contract_live/test_connector_contract.py)
  holds the real connector to the port contract the offline fakes satisfy: the
  session's role, listing an empty schema, a table's columns, the failure
  signatures, and the state table and run lock.
- [tests/integration](../../tests/integration/test_apply_live.py) and
  [tests/e2e](../../tests/e2e/test_live_lifecycle.py) publish one small
  semantic view, with a comment and two metrics, and check that it answers a
  metric, that a second plan finds nothing to do, and that a replace keeps a
  `SELECT` grant.

So the live suite confirms `COPY GRANTS` on a semantic view and that the
renderer's simplest output is accepted. It exercises nothing else on this page.
Every other entry is held offline or by the probes above.
