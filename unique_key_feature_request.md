# Feature Request: Add UNIQUE Key Constraint Support

## Problem Statement

SST currently cannot define UNIQUE key constraints on tables in semantic views, only PRIMARY KEY constraints. This limitation prevents users from creating ASOF JOIN relationships, which **Snowflake explicitly requires** to have a UNIQUE or PRIMARY KEY constraint on the combination of columns involved in the join (equality columns + time column).

### Snowflake Documentation Reference

According to the official Snowflake documentation on [Creating Semantic Views with ASOF Relationships](https://docs.snowflake.com/en/user-guide/views-semantic/sql.html#using-a-date-time-timestamp-or-numeric-range-to-join-logical-tables):

> "If there is a single UNIQUE keyword for the logical table in the TABLES clause, you don't need to specify the corresponding columns in the relationship."

The documentation example explicitly shows:

```sql
CREATE OR REPLACE SEMANTIC VIEW customer_orders_view
  TABLES ( 
    customer_address UNIQUE (ca_cust_id, ca_start_date), 
    customer UNIQUE (c_cust_id), 
    orders UNIQUE (o_ord_id)
  )
  RELATIONSHIPS (
    customer_address(ca_cust_id) REFERENCES customer,
    -- Defines an ASOF JOIN on the date columns.
    orders(o_cust_id, o_ord_date)
      REFERENCES
        customer_address(ca_cust_id, ASOF ca_start_date) 
  )
  ...
```

Note that `customer_address` table declares `UNIQUE (ca_cust_id, ca_start_date)` - **this is the exact combination of columns used in the ASOF relationship** and is required for Snowflake to accept the semantic view without warnings.

### Real-World Impact

When defining an ASOF relationship in SST:
```yaml
- name: orders_to_prior_orders_asof
  left_table: {{ table('orders') }}
  right_table: {{ table('orders') }}
  relationship_conditions:
    - "{{ column('orders', 'customer_id') }} = {{ column('orders', 'customer_id') }}"
    - "{{ column('orders', 'ordered_at') }} >= {{ column('orders', 'ordered_at') }}"
```

SST's parser correctly generates:
```sql
prior_orders(CUSTOMER_ID, ORDERED_AT) REFERENCES current_orders(CUSTOMER_ID, ASOF ORDERED_AT)
```

However, **Snowflake rejects this with a warning**: **"Must be a primary key or unique key"** because `(CUSTOMER_ID, ORDERED_AT)` is not declared as UNIQUE on the referenced table.

### Validated Solution

Manual testing in Snowflake confirms that adding the UNIQUE constraint resolves the issue:

```sql
CREATE OR REPLACE SEMANTIC VIEW SV_ORDERS_ASOF_TEST
  TABLES (
    current_orders AS ORDERS 
      PRIMARY KEY (ORDER_ID),
    prior_orders AS ORDERS 
      PRIMARY KEY (ORDER_ID)
      UNIQUE (CUSTOMER_ID, ORDERED_AT)  -- ✅ REQUIRED FIX
  )
  RELATIONSHIPS (
    current_to_prior_asof AS
      current_orders (CUSTOMER_ID, ORDERED_AT) 
        REFERENCES 
          prior_orders (CUSTOMER_ID, ASOF ORDERED_AT)
  )
  ...
```

**Verified with `GET_DDL()`**:
```sql
SELECT GET_DDL('SEMANTIC_VIEW', 'SV_ORDERS_ASOF_TEST');
-- Returns:
create or replace semantic view SV_ORDERS_ASOF_TEST
  tables (
    CURRENT_ORDERS as ORDERS primary key (ORDER_ID),
    PRIOR_ORDERS as ORDERS primary key (ORDER_ID) unique (CUSTOMER_ID,ORDERED_AT)
  )
  relationships (
    CURRENT_TO_PRIOR_ASOF as CURRENT_ORDERS(CUSTOMER_ID,ORDERED_AT) 
      references PRIOR_ORDERS(CUSTOMER_ID,asof ORDERED_AT)
  )
```

With the UNIQUE constraint, Snowflake accepts the semantic view without warnings and queries execute correctly.

## Proposed Solution

Add support for `unique_keys` field in table metadata, similar to how `primary_key` is currently handled:

**YAML Schema Enhancement (SST Format):**

```yaml
models:
  - name: orders
    description: Order fact table
    meta:
      sst:
        primary_key: order_id
        unique_keys: [customer_id, ordered_at]  # NEW FIELD - columns forming UNIQUE constraint
    columns:
      - name: order_id
        meta:
          sst:
            column_type: dimension
            data_type: TEXT
      - name: customer_id
        meta:
          sst:
            column_type: dimension
            data_type: TEXT
      - name: ordered_at
        meta:
          sst:
            column_type: time_dimension
            data_type: TIMESTAMP_NTZ
```

**Note:** This proposal supports a single UNIQUE constraint per table, which is the pattern confirmed in Snowflake documentation for ASOF relationships. Support for multiple UNIQUE constraints per table could be added in future iterations if needed.

**Generated SQL:**
```sql
CREATE SEMANTIC VIEW my_view
  TABLES (
    orders AS ANALYTICS.CORE.ORDERS
      PRIMARY KEY (ORDER_ID)
      UNIQUE (CUSTOMER_ID, ORDERED_AT)
  )
  ...
```

## Alternatives Considered

1. **Manual Workaround**: Users could bypass SST and write CREATE SEMANTIC VIEW SQL directly in Snowflake, but this defeats the purpose of SST's YAML-driven approach and loses version control benefits.

2. **Use Only PRIMARY KEY**: For simple cases, users could add composite PRIMARY KEYs, but this:
   - May not match actual table structure
   - Doesn't work when multiple UNIQUE constraints are needed
   - Can conflict with existing PRIMARY KEY definitions

3. **Skip ASOF Validation**: Ignore the warning and deploy anyway, but this:
   - Creates warnings in Snowflake UI
   - May cause unexpected query behavior
   - Is not production-ready

None of these alternatives provide a clean, maintainable solution.

## Priority

**Critical - Blocking my work**

ASOF JOINs are a core Snowflake semantic view feature for time-series analysis (e.g., "compare each order to the customer's previous order"). Without UNIQUE key support, this entire category of temporal relationships is unusable through SST.

## Impact

**Who benefits:**
- Users building time-series analytics (order history, customer behavior, event sequences)
- Any user needing temporal "as-of" joins in semantic views
- Teams migrating existing semantic views with UNIQUE constraints to SST

**Scope:**
- Medium-to-high impact: ASOF joins are a specialized but important feature
- **Blocks the ASOF JOIN parser enhancement** from being fully functional in production
- Required for feature parity with native Snowflake semantic views
- Without this feature, users cannot use ASOF relationships through SST at all

**Related Issues/PRs:**
- ASOF JOIN parser enhancement (adds `ASOF` keyword generation) - parser works correctly, but cannot be used without UNIQUE key support
- This feature is a **prerequisite dependency** for making ASOF JOINs usable in SST

## Technical Considerations

**Implementation Scope:**

1. **YAML Schema** (`core/models/table.py` or similar):
   - Add `unique_keys` field as List[List[str]] to support multiple UNIQUE constraints
   - Each constraint is a list of column names

2. **Metadata Extraction** (`core/extract/table_extractor.py`):
   - Query Snowflake information schema for UNIQUE constraints
   - Store in metadata tables (similar to how PRIMARY_KEY is stored)

3. **SQL Generation** (`core/generation/semantic_view_builder.py` lines 628-632):
   - Extend table definition generation to include UNIQUE clauses
   - Current code only handles PRIMARY KEY:
   ```python
   if table_info.get("PRIMARY_KEY"):
       primary_key_cols = self._parse_json_field(table_info["PRIMARY_KEY"], "primary_key")
       if primary_key_cols and isinstance(primary_key_cols, list):
           pk_cols = ", ".join([col.upper() for col in primary_key_cols])
           table_def += f"\n      PRIMARY KEY ({pk_cols})"
   ```
   - Need similar logic for UNIQUE_KEYS

4. **Validation**:
   - Validate that columns in UNIQUE constraints exist
   - Warn if UNIQUE key references non-existent columns
   - Check that ASOF relationships have corresponding UNIQUE constraints

## Example Usage

**SST YAML Definition (dbt models YAML format):**
```yaml
# models/marts/orders.yml
models:
  - name: orders
    description: Order fact table for ASOF join analysis
    meta:
      sst:
        primary_key: order_id
        unique_keys: [customer_id, ordered_at]  # NEW FIELD - columns forming UNIQUE constraint
    columns:
      - name: order_id
        meta:
          sst:
            column_type: dimension
            data_type: TEXT
      - name: customer_id
        meta:
          sst:
            column_type: dimension
            data_type: TEXT
      - name: ordered_at
        meta:
          sst:
            column_type: time_dimension
            data_type: TIMESTAMP_NTZ
      - name: order_total
        meta:
          sst:
            column_type: fact
            data_type: NUMBER
```

**SST Relationship Definition:**
```yaml
# snowflake_semantic_models/relationships/core.yml
relationships:
  - name: orders_to_prior_orders_asof
    left_table: {{ table('orders') }}
    right_table: {{ table('orders') }}
    relationship_conditions:
      - "{{ column('orders', 'customer_id') }} = {{ column('orders', 'customer_id') }}"
      - "{{ column('orders', 'ordered_at') }} >= {{ column('orders', 'ordered_at') }}"
```

**SST Semantic View Configuration:**
```yaml
# snowflake_semantic_models/semantic_views.yml
semantic_views:
  - name: sales_overview
    description: Sales analysis with temporal joins
    tables:
      - {{ table('orders') }}  # References orders model with unique_keys metadata
      - {{ table('customers') }}
```

**Generated SQL:**
```sql
CREATE OR REPLACE SEMANTIC VIEW SV_ORDERS
  TABLES (
    current_orders AS SST_JAFFLE_SHOP.DBT_DEV.ORDERS
      PRIMARY KEY (ORDER_ID)
      UNIQUE (CUSTOMER_ID, ORDERED_AT),
    prior_orders AS SST_JAFFLE_SHOP.DBT_DEV.ORDERS
      PRIMARY KEY (ORDER_ID)
      UNIQUE (CUSTOMER_ID, ORDERED_AT)
  )
  RELATIONSHIPS (
    orders_to_prior_orders_asof AS
      current_orders(CUSTOMER_ID, ORDERED_AT) 
        REFERENCES 
          prior_orders(CUSTOMER_ID, ASOF ORDERED_AT)
  )
  ...
```

**Result:**
- No warnings in Snowflake
- ASOF relationship fully functional
- Enables time-series metrics like "days since last order"

## Additional Context

**Related Work and Documentation:**
- **ASOF JOIN Parser Enhancement**: Successfully implemented parser that generates correct `ASOF` keyword syntax in REFERENCES clauses - ready to merge but blocked by this missing feature
- **Snowflake Official Documentation**: 
  - [Semantic Views SQL Reference - ASOF Relationships](https://docs.snowflake.com/en/user-guide/views-semantic/sql.html#using-a-date-time-timestamp-or-numeric-range-to-join-logical-tables)
  - [CREATE SEMANTIC VIEW Command Reference](https://docs.snowflake.com/en/sql-reference/sql/create-semantic-view.html)
  - [ASOF JOIN Construct Reference](https://docs.snowflake.com/en/sql-reference/constructs/asof-join.html)

**Testing Evidence:**
- Manual testing confirmed that adding `UNIQUE (CUSTOMER_ID, ORDERED_AT)` resolves Snowflake warnings
- Verified with `GET_DDL('SEMANTIC_VIEW', 'SV_ORDERS_ASOF_TEST')` that Snowflake correctly stores UNIQUE constraints
- Confirmed that SST's `semantic_view_builder.py` has no code path to generate UNIQUE clauses

**Snowflake Official Requirements:**

From [Snowflake Documentation - Semantic Views SQL Reference](https://docs.snowflake.com/en/user-guide/views-semantic/sql.html#using-a-date-time-timestamp-or-numeric-range-to-join-logical-tables):

1. **UNIQUE Constraint Requirement** (lines 384-389):
   > "If there is a single UNIQUE keyword for the logical table in the TABLES clause, you don't need to specify the corresponding columns in the relationship."

2. **ASOF JOIN Behavior** (lines 352-364):
   > "A query of the semantic view defined above produces an ASOF JOIN that uses the `>=` comparison operator in the MATCH_CONDITION clause. This joins the two tables so that the values in `col_table_1` are greater than or equal to the values in `col_table_2`."

3. **Complete Working Example** (lines 425-465):
   The documentation provides a full example showing `UNIQUE (ca_cust_id, ca_start_date)` declared on the table used in an ASOF relationship, demonstrating this is the **only supported pattern** for ASOF joins.

**Why This Matters:**
Snowflake validates that columns used in ASOF relationships have uniqueness constraints to ensure correct temporal join semantics. Without declaring the columns as UNIQUE, Snowflake cannot guarantee that the "as-of" join will return deterministic results (i.e., finding the single most recent matching row).

## Pre-submission Checklist

- [x] I have searched existing issues to avoid duplicates
- [x] I have described a clear problem and solution
- [x] I have considered alternatives and workarounds
