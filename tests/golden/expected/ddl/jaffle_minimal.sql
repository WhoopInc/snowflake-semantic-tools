-- =============================================================================
-- expected/ddl/jaffle_minimal.sql -- GOLDEN
-- =============================================================================
-- THE SMALLEST VALID SEMANTIC VIEW IN THE FIXTURE, AND THE ONLY GOLDEN WHOSE
-- SUBJECT IS ABSENCE.
--
-- Rendered from `project/semantic_models/semantic_views/semantic_views.yml`
-- view 3, whose ENTIRE declaration is three keys -- `name`, `description`,
-- `tables` -- with a single-entry table list. Everything below that is not a
-- consequence of those three keys is a consequence of ATTACHMENT, and that is
-- the whole point of this file.
--
-- *** EVERY CLAUSE SHAPE BELOW WAS VERIFIED AGAINST SNOWFLAKE BEFORE BEING
-- *** WRITTEN HERE. `TESTED` 2026-09-24 in an isolated scratch schema: this exact
-- *** statement, with only the database and schema changed, was published and
-- *** read back. It is not a statement of intent that happens to look like SQL.
--
-- WHAT THIS FILE ASSERTS THAT THE OTHER TWO CANNOT
--
--   1. NO `RELATIONSHIPS` CLAUSE, AND IT IS STRUCTURALLY IMPOSSIBLE RATHER THAN
--      MERELY ABSENT. The view has ONE table. A relationship needs two, so no
--      authoring of `relationships.yml` could put one here. This is the only
--      golden where a clause's absence cannot be changed by editing input.
--
--   2. A `PRIMARY KEY` THAT ARRIVES BY INHERITANCE AND NOT BY DECLARATION.
--      `table_config` is absent from this view, so this view declares no key --
--      and `PRIMARY KEY (PRODUCT_ID)` renders anyway, because the key is a
--      property of the `products` MODEL (`config.meta.sst.primary_key`) rather
--      than of any view's treatment of it. Decision `D231`.
--
--      > CORRECTED 2026-09-24 BY `D231`, THE SAME ERROR AS ITEM 3 BELOW AND IN
--      > THE SAME DIRECTION. This file previously asserted NO `PRIMARY KEY`,
--      > reasoning that `table_config` is absent so nothing declares one. That
--      > confused "declares nothing" with "receives nothing" -- exactly the
--      > confusion item 3 records for metrics -- and it was only ever true
--      > because a key USED to be per-view. `products` is shared with
--      > `jaffle_menu`, so its grain reaches this view whether this view
--      > mentions it or not. The inherited form asserts something STRONGER
--      > than absence would have: that a key declared once on a model reaches
--      > a view that declares no `table_config` at all.
--
--      > THE RETIRED ASSERTION IS STILL A MEASURED FACT, AND IT MOVED RATHER
--      > THAN BEING DROPPED. `TESTED` -- Snowflake accepts a semantic view
--      > whose table declares no primary key and no unique key, and separately
--      > accepts `PRIMARY KEY` on a column the view does NOT surface as a
--      > dimension (drift row 40). Neither is asserted by a
--      > positive golden any more: with the grain on the model, every model in
--      > this fixture declares its key, so the keyless path is exercised by a
--      > negative fixture instead -- which is where a warning about a MISSING
--      > declaration belongs.
--
--   3. A `METRICS` CLAUSE WITH EXACTLY ONE MEMBER, WHICH ARRIVES BY ATTACHMENT
--      AND NOT BY DECLARATION. `product_count` names no view. It refs
--      `products`, `products` is in this view's table list, so it attaches.
--
--      > CORRECTED 2026-09-24. This file previously reasoned that a view
--      > declaring only the required keys should therefore assert the absence
--      > of every optional clause, and `semantic_views.yml` still described the
--      > view that way. That was wrong, and it was wrong in an instructive
--      > direction: it confused "declares nothing optional" with "receives
--      > nothing optional". Attachment is IMPLICIT BY TABLE MEMBERSHIP, and
--      > `products` is shared with `jaffle_menu`, so a metric lands here
--      > whether this view mentions it or not. A golden asserting zero metrics
--      > would have required either an eighth model existing only to keep a
--      > comment true, or an opt-out mechanism -- and the single-metric form
--      > asserts something STRONGER than absence would have: that attachment
--      > reaches a single-table view at all.
--
--   4. NO `AI_VERIFIED_QUERIES`, AND THE NEAR MISS IS THE INTERESTING PART.
--      `product_mix_by_type` selects `products.product_type`, so a reader
--      scanning for "does any VQ touch products" would conclude one attaches.
--      None does: that query also joins `order_items` and `orders`, neither of
--      which is in this view, and a member attaches only when ALL its tables
--      are present. The absence is a JOIN-ARITY fact, not a products fact.
--
--   5. NO `FILTERS` OF ANY KIND. All four filters in `filters.yml` declare
--      `tables: [orders]`. `orders` is not here, so none attaches -- neither as
--      a `LABELS = (FILTER)` dimension nor as appended prose.
--
--   6. NO `WITH EXTENSION (CA=...)`, AND THAT IS NOW TRUE OF EVERY GOLDEN.
--      `TESTED` 2026-09-24: a view whose sample values are NATIVE emits no
--      extension clause at all on read-back. The clause is deleted from 1.0 --
--      decision `D220`, and drift finding 3 is SCOPED by
--      that decision rather than overturned.
--
--   7. NO VARIABLES, NO TAGS, NO MAX_STALENESS, NO CUSTOM INSTRUCTIONS. None is
--      declared and none attaches. `AI_SQL_GENERATION` and
--      `AI_QUESTION_CATEGORIZATION` are absent because custom instructions
--      attach EXPLICITLY -- the view names them -- and this view names none.
--      That is the one member type table membership does not reach.
--
-- SAMPLE VALUES AND ENUM FLAGS ARE NATIVE CLAUSES HERE, NOT PAYLOAD KEYS
--
--   `SAMPLE_VALUES` is valid on BOTH dimensions and facts -- `LIST_PRICE` below
--   is a fact carrying it, which is the assertion. `IS_ENUM` is DIMENSIONS-ONLY
--   and takes no value: it is emitted for `is_enum: true` and OMITTED for
--   `is_enum: false`, which is why only `PRODUCT_TYPE` carries it while
--   `PRODUCT_ID` and `PRODUCT_NAME` -- both authored `is_enum: false` -- do not.
--   Where both appear, `SAMPLE_VALUES` MUST PRECEDE `IS_ENUM`.
--
-- EXPECTED DIAGNOSTICS: NONE. This golden renders clean, and it did not before.
--
--   CORRECTED 2026-09-24 BY `D231`. This file previously asserted `V012` [W],
--   described as "a view with no relationships". That was wrong twice over. The
--   description named a rule that does not exist -- `V012` is *"a table declares
--   neither primary_key nor unique_keys"*,
--   and NO rule warns about a relationship-less view, which is why item 1 above
--   can call the absence unavoidable rather than diagnosable. The CODE was right
--   for the wrong reason: `products` really did declare no key here, because a
--   key was per-view and this view has no `table_config`. Under `D231` the key
--   is on the model, so `V012` no longer fires and `expected/diagnostics.txt`
--   now reports every table in ALL THREE views as keyed.
--
-- *** THIS FILE IS NOT WHAT `GET_DDL` RETURNS, AND MUST NOT BE COMPARED TO IT
-- *** RAW. `TESTED` 2026-09-24, recorded as drift finding 3a: `GET_DDL`
-- *** lowercases every keyword, rewrites `WITH SYNONYMS ('a', 'b')` to
-- *** `with synonyms=('a','b')` -- inserting `=`, stripping the comma spacing --
-- *** while leaving `sample_values ('x', 'y')` spaced and un-`=`-ed, and omits
-- *** `COPY GRANTS` entirely. Two clauses authored alike come back unalike, so
-- *** the drift normaliser cannot be inferred from one of them.
-- =============================================================================

CREATE OR REPLACE SEMANTIC VIEW SST_REF_DEV.JAFFLE.JAFFLE_MINIMAL
  TABLES (
    PRODUCTS AS SST_REF_DEV.JAFFLE.PRODUCTS PRIMARY KEY (PRODUCT_ID)
  )
  FACTS (
    PRODUCTS.LIST_PRICE AS PRODUCTS.LIST_PRICE WITH SYNONYMS ('menu price', 'catalogue price', 'sticker price') COMMENT = 'Menu price for one unit, in cents.' SAMPLE_VALUES ('1100', '700')
  )
  DIMENSIONS (
    PRODUCTS.PRODUCT_ID AS PRODUCTS.PRODUCT_ID WITH SYNONYMS ('product key', 'menu item identifier') COMMENT = 'Surrogate key for the product. An identifier, therefore a dimension.',
    PRODUCTS.PRODUCT_NAME AS PRODUCTS.PRODUCT_NAME WITH SYNONYMS ('menu item', 'item name', 'dish') COMMENT = 'The menu item''s name as printed on the receipt.' SAMPLE_VALUES ('nutellaphone who dis', 'doctor stew'),
    PRODUCTS.PRODUCT_TYPE AS PRODUCTS.PRODUCT_TYPE WITH SYNONYMS ('item category', 'menu category', 'food or drink') COMMENT = 'Whether the item is food or drink. A closed set.' SAMPLE_VALUES ('jaffle', 'beverage') IS_ENUM
  )
  METRICS (
    PRODUCTS.PRODUCT_COUNT AS COUNT(DISTINCT PRODUCTS.PRODUCT_ID) COMMENT = 'Number of distinct products sold.'
  )
  COMMENT = 'Menu products only. The smallest valid semantic view in this fixture.'
  COPY GRANTS
