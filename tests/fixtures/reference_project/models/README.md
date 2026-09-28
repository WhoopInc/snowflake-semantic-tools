# `project/models/` -- THE DBT LAYER OF THE REFERENCE FIXTURE

**ONE `.sql` AND ONE `.yml` PER MODEL, SIDE BY SIDE, WHICH IS THE `analytics-dbt`
PRODUCTION CONVENTION.** The fixture previously carried one consolidated
`_marts__models.yml` describing seven models. That is legal dbt and it is NOT how the
production repo is laid out, and the gap mattered more than it looks: the fixture's
whole job is to be the thing an engineer reads before writing the engine, so a layout
no production project uses teaches the wrong shape.

**AND THE OLD LAYOUT WAS NOT MERELY UNCONVENTIONAL -- IT WAS BROKEN, for a reason the
file itself got backwards.** The deleted `_staging__models.yml` argued that most
models needed no `.sql` because "SST reads the compiled manifest, never the SQL" and
seven extra files "would assert nothing additional". The first clause is true and the
conclusion does not follow from it, because of what makes a dbt model exist:

> **A DBT MODEL IS DEFINED BY ITS `.sql` FILE, NOT BY ITS YAML.** A `models:` entry
> naming a model with no corresponding `.sql` is a PATCH WITH NOTHING TO PATCH. dbt
> emits `Did not find matching node for patch with name '<model>'`, and the model
> DOES NOT ENTER THE MANIFEST.

So the old fixture's `dbt parse` would have produced a manifest containing exactly ONE
model -- `stg_orders`, the only one with SQL -- and every `{{ ref('orders') }}`,
`{{ ref('customers') }}` and so on in the semantic layer would have failed to resolve
against it. The argument was self-defeating: it correctly said SST reads the manifest,
then removed the one thing that puts a model IN the manifest.

That is the second independent way the fixture could not have produced its own
goldens, and it is worth stating next to the first (the goldens asserting a different
domain from the input), because they have the same root: **an input surface and an
output surface were each checked against the spec and never against each other.**

`sst` itself is still indifferent to these files -- it reads `target/manifest.json` --
so the split changes what a READER sees, and the `.sql` files change what dbt PRODUCES.

**WHAT LIVES WHERE NOW:**

| File | Holds |
|---|---|
| `marts/<model>.sql` | the model's SQL, one per model |
| `marts/<model>.yml` | that model's `description`, `config.meta.sst` and `columns` |
| `staging/_sources.yml` | the `sources:` declaration, which belongs to no single model |
| `staging/stg_orders.{sql,yml}` | the one staging model |
| this file | everything that is true of the DIRECTORY rather than of one model |

**WHY THIS FILE EXISTS RATHER THAN A HEADER REPEATED SEVEN TIMES.** The reasoning
below is cross-cutting: it is about the seam, the domain, and the data rule, none of
which belong to `customers` more than to `orders`. Copying it into seven files would
make seven places to update and six of them would go stale. Each per-model `.yml`
carries a SHORT header naming only what that model uniquely asserts.

dbt parses `.md` files in a model path looking for doc blocks. This file contains none
and dbt ignores it. It is documentation for people.

THE SENTENCE ABOVE USED TO SPELL THE TAG OUT, and doing so BROKE `dbt parse` for the
whole project: `Compilation Error: Reached EOF without finding a close tag for docs
(searched from line 51)`. A file explaining that dbt looks for doc blocks here had
written one, in prose, with no closing tag -- so the claim "finds none here" was false
because of the way it was written down. The tag is now described rather than quoted;
{% raw %}{% raw %}{% endraw %}{% endraw %} would also work and is noisier to read.

**NOTHING IN THIS PLAN CAUGHT IT, AND NOTHING COULD HAVE.** All eight instruments are
offline text checks over the plan tree; a Jinja parse error in a fixture README is only
visible to dbt. It sat here until dbt was actually installed and run, which is the
argument for running the fixture rather than reasoning about it.

---

models/ -- the reference-impl fixture
THE SEAM. The dbt model is the source of truth for columns: `sst enrich`
derives every dimension and fact from what is declared here, and a semantic
view is authored as A LIST OF TABLES with everything else compiled.

SST never opens this file. It reads target/manifest.json.

THE DOMAIN IS JAFFLE-INSPIRED, NOT COPIED (`D216`). A jaffle shop sells
toasted sandwiches from a handful of locations. The domain was chosen because
a reader meets it already understanding it, which is the whole job of a
reference fixture -- and because its grain gives every join kind and every
metric shape a real home instead of a contrived one. An earlier Jaffle Shop
fixture is the design donor; nothing was copied from it -- in particular, none
of its `sample_values` are here.

EVERY `description:` IS ONE LINE, AND THAT IS A GOLDEN-FILE CONSTRAINT
A column description becomes the COMMENT on the derived dimension or fact, and
the renderer builds ONE LOGICAL ROW PER MEMBER. A multi-line description puts a
literal newline INSIDE the single-quoted SQL string, so the member's row spans
several lines of DDL and the golden file stops being one-member-per-line.

That is not wrong -- it is faithful -- but it makes the assertion much harder to
read, and being auditable by reading is the entire point of this deliverable. So
every `description:` below is a single `|-` line. The teaching prose lives in `#`
comments, which is where it belongs: a dbt description is shipped to Snowflake as
a COMMENT and read by Cortex Analyst, so it should be a crisp statement about the
column, not an essay about tooling.

THE DATA RULE, AS IT APPLIES TO THIS FILE (`D213`)
`sample_values:` IS AUTHORED ON TWENTY-TWO OF THIS FILE'S THIRTY-SEVEN COLUMNS AND
ABSENT FROM THE OTHER TWENTY-NINE, AND BOTH HALVES ARE ASSERTIONS.

WHAT CHANGED AT `D213`. The values below are real jaffle vocabulary -- product
names, order states, location names -- not the opaque `SYNTHETIC_*` tokens an
earlier draft used. That is permitted because jaffle data is dbt Labs' PUBLISHED
fixture: there is no member whose row could appear in it. What is still banned is
COLLECTION, and the distinction is the whole rule. `sst enrich --include
sample-values` SELECTs DISTINCT from the real column and writes what it finds
into this tracked file -- it is the one path this tooling has for moving real
data into a git repository, and `allow_sample_value_collection: false` (`C021`)
keeps it shut. Authoring a literal by hand moves nothing.

NO IDENTIFIER-SHAPED VALUE APPEARS, and that is not a formality. Sample values a
tool writes from a real build can carry genuine identifiers -- UUID v4 strings,
say -- or artefacts such as a literal `nan` from a pandas round-trip, and a
reviewer reading a large diff will not reliably catch them. A value being synthetic does not
make an identifier shape safe to put in front of a reader who will copy it.

WHY TWENTY-TWO AND NOT ALL THIRTY-SEVEN. Sample values render as a NATIVE per-member
`SAMPLE_VALUES (...)` clause, emitted only for members that carry the key: authoring
none makes the correct output NO `SAMPLE_VALUES` ANYWHERE, while the goldens assert
several. Authoring all of them destroys the other half -- an UNAUTHORED column is the
only input that can catch a renderer inventing a slot it was never given. Twenty-two is a
set that keeps both assertions live -- the fifteen unauthored columns, twelve of them
identifiers and all five of `order_items`, are what keep the absence side checkable.

> **THE FIGURE IN THIS SECTION READ "EIGHT" UNTIL 2026-09-24 AND WAS STALE BY FOURTEEN.**
> It was written when the fixture was smaller and nothing recomputed it when columns were
> added. Measured on disk: customers 3, orders 5, locations 3, products 3, pricing_periods 5,
> supplies 3, order_items 0. Same defect class as the stale goldens and the stale drift
> tally -- a derived number with no instrument behind it. THIS PARAGRAPH USED TO DESCRIBE A
WHOLE-CLAUSE GATE: `has_any_sample_values` deciding whether one
`WITH EXTENSION (CA=...)` clause appeared for the entire view. `D220` deletes that
clause. The gate is per member now, and the pair of assertions is unaffected.

`is_enum` IS EXERCISED HERE, AND IT IS A LITERAL BOOLEAN
`is_enum: true` was previously unexercisable: the no-values rule blocked it and
`audit_sst.py` check 6 hard-fails on `is_enum` carrying anything that is not a
literal boolean -- because a STRING there is a data value wearing a flag's name.
The check always exempted the literal, so `D213` is what made the happy path
available rather than the check changing.

---

## THE COMPLETE `config.meta.sst` SURFACE, AND WHY SOME KEYS ARE ABSENT

**COLUMN LEVEL -- ALL FIVE KEYS ARE EXERCISED, ON DELIBERATELY DIFFERENT SUBSETS:**

| Key | Columns | Why that count |
|---|---:|---|
| `column_type` | 37 of 37 | authored on every column, always |
| `synonyms` | 32 | every column of the six populated marts |
| `sample_values` | 22 | every column of those six EXCEPT identifiers and `customer_name` |
| `is_enum` | 17 | DIMENSIONS only -- never a fact. A TIMESTAMP column is a PLAIN dimension whose `DATA_TYPE` carries its time-ness (`D219` retired `time_dimension` as a category), and none carries `is_enum` because a timestamp is not a closed set |
| `data_type` | 5 | all of `supplies`, the one model with no contract |

**FIFTEEN COLUMNS CARRY NO `sample_values`, AND EVERY ONE IS A PRINCIPLED
EXCLUSION RATHER THAN AN OMISSION:**

| Excluded | Count | Rule |
|---|---:|---|
| `*_id` identifiers | 12 | **no identifier-shaped value appears** -- the rule stated below |
| `customer_name` | 1 | **a person's name is personal data.** `product_name` and `location_name` DO carry values, and the asymmetry is the point: those name things, this names someone |
| `order_items.occurred_at`, `.item_price` | 2 | the deliberately-bare model |

**TIMESTAMP COLUMNS DO CARRY `sample_values`, AND THEY STILL HAVE TO -- FOR A
DIFFERENT REASON THAN THIS SECTION USED TO GIVE.** All six timestamps outside
`order_items` carry two ISO-8601 values. The old reason was that without at least one
of them the `time_dimensions` array in the `CA=` payload was UNREACHABLE, and it cited
a real defect: an earlier rebuild of the payloads omitted every timestamp and silently
dropped an array the previous golden had exercised. That observation stands. The array
does not. `D219` DELETES it -- not as unreachable but as REDUNDANT, because time-ness
is read off `DATA_TYPE`, and `D220` deletes the payload that held it. SNOWFLAKE HAS NO
TIME-DIMENSION CONCEPT AT ALL: there is no `TIME_DIMENSIONS` clause, there is no
`time_dimensions` YAML key, and `DESCRIBE SEMANTIC VIEW` reports these columns as
`DIMENSION ... DATA_TYPE DATE`. What the values feed now is the native
`SAMPLE_VALUES` clause on each member. `pricing_periods` is still the sharpest case,
and its `effective_end_at` values deliberately reuse the next period's
`effective_start_at` because the intervals are half-open.

**THE MARTS SHOW THREE `meta.sst` SHAPES, AND THE SPREAD IS THE ASSERTION.** Six marts are fully
populated; `order_items` carries NOTHING BUT `column_type`; and `product_docs` carries NO `meta.sst`
AT ALL. Eight marts, three shapes.

That replaced an earlier design in which metadata was scattered thinly across all seven models.
Scattering was worse on both counts: it made every file look half-finished, and it asserted nothing a
single deliberately-bare model does not assert better.

**THE THIRD SHAPE IS THE NEWEST AND THE EASIEST TO MISREAD AS AN OVERSIGHT.** `product_docs` has no
`meta.sst` on any column because NO SEMANTIC VIEW REFERENCES IT -- it exists only to back the
`menu_docs_search` Cortex Search service. `SST-VAL308` is an ERROR when a column *the semantic layer
consumes* has no `column_type`, and `semantic_views.yml` lists TABLES rather than members, so
membership follows from whether a view names the table. A table no view names consumes nothing and
needs nothing. Authoring `column_type` there anyway would assert that a search-only relation is part
of the semantic layer, which is the opposite of why it was split out of `products`.

**WHAT THE BARE MODEL CATCHES THAT A POPULATED ONE CANNOT.** The renderer gates on
PRESENCE, PER MEMBER: a column carrying only `column_type` renders as name, expression
and `COMMENT` and stops -- no `WITH SYNONYMS`, no `SAMPLE_VALUES`, no `IS_ENUM`. A
publisher that emitted those clauses unconditionally -- `WITH SYNONYMS ()`,
`SAMPLE_VALUES ()`, `IS_ENUM` everywhere -- would pass a
fixture in which every column is populated, because no column's absence could be
checked. `order_items` is what makes OMITTED distinguishable from EMPTY. THE GATE
DESCRIBED HERE USED TO BE A WHOLE-CLAUSE ONE: `has_any_sample_values` deciding whether
a `WITH EXTENSION (CA=...)` clause was emitted at all, with `ORDER_ITEMS` appearing in
the payload with its member arrays ABSENT rather than present-and-empty. `D220` deletes
that clause; the per-member gate is what remains, and it is what this model exercises.

**`is_enum` IS ON DIMENSIONS ONLY, AND ITS ABSENCE FROM FACTS IS ALSO AN
ASSERTION.** An enum verdict on a number is meaningless: `order_total` has sampled
values and is not a closed set in any useful sense, so the key is omitted rather
than set to `false`. The `false` values that ARE authored sit on identifier columns
and on `location_name` and `product_name` -- dimensions whose sets are open -- so
the fixture distinguishes "not an enum" from "enum-ness does not apply".

**`data_type`: THE FIXTURE COVERS BOTH RESOLUTION PATHS BECAUSE PRODUCTION USES THE
FALLBACK ALMOST EXCLUSIVELY.** Six marts enforce a contract and declare a native
`data_type:`; `supplies` enforces none and declares `meta.sst.data_type` instead. The
shapes are mutually exclusive per model, since a contract requires the native field.
Covering only the contract shape would have meant exercising the path the reference
consumer NEVER takes -- native `data_type` is empty on every one of its model
columns, while `meta.sst.data_type` is populated on nearly all of them.

**MODEL LEVEL -- A NARROW `meta.sst` BLOCK, AND WHAT IS AND IS NOT IN IT:**

Seven of the eight models in this directory declare a model-level `config.meta.sst`,
and each carries nothing but the model's GRAIN. (`product_docs` declares none: it is
read by no semantic view.) Three keys a reader coming from `analytics-dbt` will expect
there are gone, each for its own reason -- and two they may expect to have left are
still here:

| Expected key | What happened to it |
|---|---|
| `cortex_searchable` | **REMOVED IN v0.3.0.** Warns and is ignored. Production still carries it on many models and every one of those is a no-op. |
| `primary_key` | **STILL HERE, AND THIS IS THE SINGLE SOURCE.** Declared at `config.meta.sst.primary_key`. An earlier draft moved it to `table_config.<model>.primary_key` in `semantic_views.yml`; decision `D231` reversed that. |
| `unique_keys` | **STILL HERE**, at `config.meta.sst.unique_keys`, for the same reason. Only `customers` declares one. |
| `synonyms` (table level) | MOVED to `table_config.<model>.synonyms`. The column-level `synonyms` above is a DIFFERENT key and is still here. |
| `database` / `schema` | **FORBIDDEN IN 1.0.** Rejected with `SST-DBT030`. Decision `D217`. |

**THREE KINDS OF CHANGE, NOT TWO: A RELOCATION, A RETENTION, AND A REJECTION.**

Table-level `synonyms` MOVED because it is PRESENTATION -- it states what a reader of
ONE view may call this table, and two views can reasonably differ. With `name` and
`distinct_range` it is what `table_config` now holds, and all three describe how a
VIEW treats a table.

`primary_key` and `unique_keys` DID NOT MOVE, because they are not about a view's
treatment of anything. They state the GRAIN OF THE MODEL, and a model has one grain.
The draft that put them in `table_config` had to argue that the same dbt model
appearing in more than one view was a reason to let each view state its own -- and
that argument does not survive contact with this fixture. `orders` is in BOTH
`jaffle_sales` and `jaffle_menu`, and under the per-view placement it declared
`primary_key: [order_id]` TWICE, identically. The fixture's own comment said so, and
called the duplication "a property of the dialect". It was a property of the placement.
The shape that placement could express and this one cannot -- two different primary
keys for one model -- has no meaning, so being unable to write it is a gain.

**A KEY DECLARED HERE REACHES EVERY VIEW THAT READS THE MODEL, AND THAT IS THE POINT.**
`products` is read by `jaffle_menu`, which authors a `table_config`, and by
`jaffle_minimal`, which authors none at all. Both goldens render
`PRIMARY KEY (PRODUCT_ID)`. A view that declares nothing still RECEIVES what its
models declare -- the same distinction `expected/ddl/jaffle_minimal.sql` already draws
for metric attachment.

`V026` (no column in both lists) and `SST-VAL310` (every named column exists) are
enforced against the declaration here, and neither was weakened by the move back:
both resolve against the dbt catalog scoped to the
model, which is what this placement names directly rather than by way of a map key.

`database` and `schema` were not relocated, because there was nowhere for them to go
and nothing for them to do. **They never supplied anything.** SST reads the RESOLVED
location out of `target/manifest.json`, and dbt has applied `+database` and
`+schema` before that file exists. An earlier draft of this plan kept them as a
drift-detection assertion -- an independent second copy for `B007` to compare -- and **BOTH THE KEYS AND `B007` ARE NOW GONE (`D217`, withdrawn)**, and
that argument does not survive being stated plainly: **the manifest IS the compiled
dbt config**, so the comparison could only ever catch a hand-written value
disagreeing with the truth, which is an error class that exists SOLELY BECAUSE THE
KEY EXISTS. Its companion `B008` warned against populating the key, which was the
key's only sanctioned use. A key that warns when set and does nothing when null has
no correct usage, so 1.0 rejects it.

**THIS DIRECTORY USED TO DECLARE THEM AS `null` ON ALL EIGHT MODELS, AND THAT WAS
WORSE THAN IT LOOKED.** `MEASURED` 2026-09-23 against a large production dbt project:
nearly every model **omits both keys**, **a handful hardcode them**, and **ZERO
declare them as null**. The fixture was teaching a shape with no counterpart anywhere
in the reference consumer -- and because `B007` (now WITHDRAWN, `D217`) only evaluated "when present" and
`B008` only fired on hardcoding, eight models of `null` exercised NEITHER rule. Two
of the fifteen seam rules had no case anywhere in the corpus and no instrument
reported it. `negative/18-forbidden-meta-sst-location.yml` is now the only place the LOCATION keys
appear, and it asserts the rejection. The grain keys are a different matter: they are
declared on seven of the eight models here, and `D231` is why.

**SO A MODEL FILE HERE THAT LOOKS THINNER THAN ITS PRODUCTION COUNTERPART IS NOT
INCOMPLETE.** It is the 1.0 surface -- `config.meta.sst` carries the grain and nothing
else. The legacy shape, with `cortex_searchable` and table-level `synonyms` at model
level too, is preserved for comparison in `examples/semantic_views/`.
