"""Unit tests for the pure renderer -- the behaviours the golden cannot isolate.

The golden proves the whole statement is right for one real view. These prove the
individual rules hold in cases the fixture does not happen to contain: an apostrophe
inside a comment, an empty clause, a filter's modifier order, a metric with no owning
table. Each is a rule a future change could break without disturbing the golden.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.semantic_view import (
    Column,
    ColumnKind,
    Metric,
    Relationship,
    SemanticView,
    Table,
    Tag,
    Variable,
    VerifiedQuery,
)
from snowflake_semantic_tools.domain.render.semantic_view import (
    quote,
    render,
    render_column,
    render_metric,
    render_relationship,
    render_table,
    render_variable,
    render_verified_query,
)

PRODUCTS = Table(logical_name="PRODUCTS", fqn="DB.SCH.PRODUCTS", primary_key=("PRODUCT_ID",))


def minimal(**overrides: object) -> SemanticView:
    base: dict[str, object] = {"fqn": "DB.SCH.V", "tables": (PRODUCTS,)}
    base.update(overrides)
    return SemanticView(**base)  # type: ignore[arg-type]


class TestQuoting:
    def test_doubles_an_embedded_single_quote(self) -> None:
        assert quote("the item's name") == "'the item''s name'"

    def test_doubles_every_occurrence_not_just_the_first(self) -> None:
        assert quote("a'b'c") == "'a''b''c'"

    def test_keeps_newlines_literal(self) -> None:
        """The golden's multi-line COMMENT spans lines inside one literal."""
        assert quote("one\ntwo") == "'one\ntwo'"

    def test_empty_string_is_still_quoted(self) -> None:
        assert quote("") == "''"


class TestTables:
    def test_primary_key_and_unique_and_synonyms_in_order(self) -> None:
        table = Table(
            logical_name="CUSTOMERS",
            fqn="DB.SCH.CUSTOMERS",
            primary_key=("CUSTOMER_ID",),
            unique_keys=(("CUSTOMER_NAME",),),
            synonyms=("buyer", "guest"),
        )
        assert render_table(table) == (
            "CUSTOMERS AS DB.SCH.CUSTOMERS PRIMARY KEY (CUSTOMER_ID) "
            "UNIQUE (CUSTOMER_NAME) WITH SYNONYMS ('buyer', 'guest')"
        )

    def test_composite_key_is_comma_separated(self) -> None:
        table = Table(logical_name="S", fqn="DB.SCH.S", primary_key=("PRODUCT_ID", "SNAPSHOT_MONTH"))
        assert "PRIMARY KEY (PRODUCT_ID, SNAPSHOT_MONTH)" in render_table(table)

    def test_omits_absent_parts_entirely(self) -> None:
        assert render_table(Table(logical_name="T", fqn="DB.SCH.T")) == "T AS DB.SCH.T"

    def test_distinct_range_follows_keys(self) -> None:
        table = Table(
            logical_name="PERIODS",
            fqn="DB.SCH.PERIODS",
            primary_key=("ID",),
            distinct_range=("START_AT", "END_AT"),
        )
        assert render_table(table) == (
            "PERIODS AS DB.SCH.PERIODS PRIMARY KEY (ID) "
            "CONSTRAINT PERIODS_DISTINCT_RANGE DISTINCT RANGE BETWEEN START_AT AND END_AT EXCLUSIVE"
        )

    def test_tables_keep_declaration_order(self) -> None:
        """Authored order, NOT sorted -- unlike every other member list."""
        a = Table(logical_name="ORDERS", fqn="DB.SCH.ORDERS")
        b = Table(logical_name="CUSTOMERS", fqn="DB.SCH.CUSTOMERS")
        out = render(minimal(tables=(a, b)))
        assert out.index("ORDERS AS") < out.index("CUSTOMERS AS")


class TestColumns:
    def test_full_modifier_order(self) -> None:
        col = Column(
            table="PRODUCTS",
            name="PRODUCT_TYPE",
            kind=ColumnKind.DIMENSION,
            expr="PRODUCTS.PRODUCT_TYPE",
            comment="Food or drink.",
            synonyms=("item category",),
            sample_values=("jaffle", "beverage"),
            is_enum=True,
        )
        assert render_column(col) == (
            "PRODUCTS.PRODUCT_TYPE AS PRODUCTS.PRODUCT_TYPE "
            "WITH SYNONYMS ('item category') COMMENT = 'Food or drink.' "
            "SAMPLE_VALUES ('jaffle', 'beverage') IS_ENUM"
        )

    def test_is_enum_false_renders_nothing(self) -> None:
        col = Column(table="T", name="C", kind=ColumnKind.DIMENSION, expr="T.C", is_enum=False)
        assert "IS_ENUM" not in render_column(col)

    def test_filter_label_precedes_AS(self) -> None:
        """`LABELS = (FILTER)` qualifies the NAME, so it sits before `AS`."""
        col = Column(
            table="ORDERS",
            name="IS_COMPLETED_ORDER",
            kind=ColumnKind.FILTER,
            expr="ORDERS.ORDER_STATE = 'completed'",
        )
        assert render_column(col) == ("ORDERS.IS_COMPLETED_ORDER LABELS = (FILTER) AS ORDERS.ORDER_STATE = 'completed'")

    def test_filters_render_inside_DIMENSIONS_not_their_own_clause(self) -> None:
        """Review finding 1.1: there is no `FILTERS (` clause head."""
        view = minimal(
            columns=(
                Column(table="T", name="D", kind=ColumnKind.DIMENSION, expr="T.D"),
                Column(table="T", name="F", kind=ColumnKind.FILTER, expr="T.X = 1"),
            )
        )
        out = render(view)
        assert "FILTERS (" not in out
        assert "DIMENSIONS (" in out
        assert "T.F LABELS = (FILTER)" in out

    def test_members_are_sorted_by_qualified_name(self) -> None:
        view = minimal(
            columns=(
                Column(table="T", name="Z", kind=ColumnKind.DIMENSION, expr="T.Z"),
                Column(table="T", name="A", kind=ColumnKind.DIMENSION, expr="T.A"),
            )
        )
        out = render(view)
        assert out.index("T.A AS") < out.index("T.Z AS")

    def test_facts_and_dimensions_are_partitioned_by_kind(self) -> None:
        view = minimal(
            columns=(
                Column(table="T", name="F", kind=ColumnKind.FACT, expr="T.F"),
                Column(table="T", name="D", kind=ColumnKind.DIMENSION, expr="T.D"),
            )
        )
        facts = render(view).split("FACTS (")[1].split(")")[0]
        assert "T.F" in facts and "T.D" not in facts


class TestMetrics:
    def test_metric_with_a_table_renders_qualified(self) -> None:
        m = Metric(name="ORDER_COUNT", expr="COUNT(1)", table="ORDERS", comment="n")
        assert render_metric(m) == "ORDERS.ORDER_COUNT AS COUNT(1) COMMENT = 'n'"

    def test_cross_table_metric_renders_unqualified(self) -> None:
        """As the golden's REVENUE_PER_CUSTOMER does, drawing on two tables."""
        m = Metric(name="REVENUE_PER_CUSTOMER", expr="DIV0(A, B)", table=None)
        assert render_metric(m) == "REVENUE_PER_CUSTOMER AS DIV0(A, B)"

    def test_path_and_non_additive_modifiers_precede_as(self) -> None:
        metric = Metric(
            name="M",
            expr="SUM(T.X)",
            table="T",
            using_relationships=("T_TO_D",),
            non_additive_by=("SNAPSHOT_MONTH",),
        )
        assert render_metric(metric) == ("T.M USING (T_TO_D) NON ADDITIVE BY (SNAPSHOT_MONTH) AS SUM(T.X)")

    def test_private_metric_emits_the_non_default_access_modifier(self) -> None:
        metric = Metric(name="M", table="T", expr="COUNT(*)", access_modifier="private_access")
        assert render_metric(metric).startswith("PRIVATE T.M AS")


class TestClauseOmission:
    def test_empty_clauses_are_omitted_not_rendered_empty(self) -> None:
        out = render(minimal())
        for head in ("RELATIONSHIPS", "VARIABLES", "FACTS", "DIMENSIONS", "METRICS", "AI_"):
            assert head not in out

    def test_bare_minimum_is_head_tables_and_copy_grants(self) -> None:
        assert render(minimal()) == (
            "CREATE OR REPLACE SEMANTIC VIEW DB.SCH.V\n"
            "  TABLES (\n"
            "    PRODUCTS AS DB.SCH.PRODUCTS PRIMARY KEY (PRODUCT_ID)\n"
            "  )\n"
            "  COPY GRANTS"
        )

    def test_copy_grants_is_unconditional(self) -> None:
        assert render(minimal()).endswith("COPY GRANTS")

    def test_ownership_marker_appends_to_or_becomes_comment(self) -> None:
        with_comment = minimal(comment="Human", ownership_marker="[sst:a:b]")
        marker_only = minimal(ownership_marker="[sst:a:b]")
        assert "COMMENT = 'Human [sst:a:b]'" in render(with_comment)
        assert "COMMENT = '[sst:a:b]'" in render(marker_only)

    def test_if_not_exists_replaces_nothing_and_adds_the_clause(self) -> None:
        out = render(minimal(or_replace=False, if_not_exists=True))
        assert out.startswith("CREATE SEMANTIC VIEW IF NOT EXISTS DB.SCH.V")


class TestFullClauseOrder:
    """The clause order is the contract; this pins it as a single assertion."""

    def test_every_clause_appears_in_the_verified_order(self) -> None:
        view = minimal(
            relationships=(
                Relationship(name="R", from_table="A", from_columns=("X",), to_table="B", to_columns=("Y",)),
            ),
            variables=(Variable(name="V", data_type="NUMBER", default="1"),),
            columns=(
                Column(table="T", name="F", kind=ColumnKind.FACT, expr="T.F"),
                Column(table="T", name="D", kind=ColumnKind.DIMENSION, expr="T.D"),
            ),
            metrics=(Metric(name="M", expr="COUNT(1)", table="T"),),
            comment="c",
            ai_sql_generation="gen",
            ai_question_categorization="cat",
            verified_queries=(VerifiedQuery(name="Q", question="q?", sql="SELECT 1"),),
            max_staleness="300 seconds",
            tags=(Tag(name="DB.SCH.COST_CENTER", value="analytics"),),
        )
        out = render(view)
        order = [
            "CREATE OR REPLACE SEMANTIC VIEW",
            "TABLES (",
            "RELATIONSHIPS (",
            "VARIABLES (",
            "FACTS (",
            "DIMENSIONS (",
            "METRICS (",
            "COMMENT = ",
            "AI_SQL_GENERATION ",
            "AI_QUESTION_CATEGORIZATION ",
            "AI_VERIFIED_QUERIES (",
            "MAX_STALENESS = ",
            "WITH TAG (",
            "COPY GRANTS",
        ]
        positions = [out.index(token) for token in order]
        assert positions == sorted(positions), f"clauses out of order: {out}"


class TestVerifiedQueries:
    def test_optional_parts_are_omitted_when_absent(self) -> None:
        view = minimal(verified_queries=(VerifiedQuery(name="Q", question="q?", sql="SELECT 1"),))
        out = render(view)
        assert "VERIFIED_AT" not in out
        assert "ONBOARDING_QUESTION" not in out
        assert "VERIFIED_BY" not in out

    def test_boolean_renders_as_a_sql_keyword_not_python(self) -> None:
        view = minimal(
            verified_queries=(VerifiedQuery(name="Q", question="q?", sql="SELECT 1", onboarding_question=True),)
        )
        assert "ONBOARDING_QUESTION TRUE" in render(view)
        assert "True" not in render(view)

    def test_all_optional_parts_render_when_present(self) -> None:
        query = VerifiedQuery(
            name="Q",
            question="q?",
            sql="SELECT 1",
            verified_at=1,
            verified_by="owner",
            onboarding_question=False,
        )
        rendered = render_verified_query(query)
        assert "VERIFIED_AT 1" in rendered
        assert "ONBOARDING_QUESTION FALSE" in rendered
        assert "VERIFIED_BY 'owner'" in rendered


class TestRelationshipsAndVariables:
    def test_relationship_renders_both_sides(self) -> None:
        rel = Relationship(
            name="ORDERS_TO_CUSTOMERS",
            from_table="ORDERS",
            from_columns=("CUSTOMER_ID",),
            to_table="CUSTOMERS",
            to_columns=("CUSTOMER_ID",),
        )
        assert render_relationship(rel) == (
            "ORDERS_TO_CUSTOMERS AS ORDERS (CUSTOMER_ID) REFERENCES CUSTOMERS (CUSTOMER_ID)"
        )

    def test_asof_modifier_marks_the_temporal_target_column(self) -> None:
        relationship = Relationship(
            name="ITEMS_TO_ORDERS",
            from_table="ITEMS",
            from_columns=("ORDER_ID", "OCCURRED_AT"),
            to_table="ORDERS",
            to_columns=("ORDER_ID", "ORDERED_AT"),
            asof_index=1,
        )
        assert render_relationship(relationship) == (
            "ITEMS_TO_ORDERS AS ITEMS (ORDER_ID, OCCURRED_AT) " "REFERENCES ORDERS (ORDER_ID, ASOF ORDERED_AT)"
        )

    def test_range_relationship_renders_half_open_bounds(self) -> None:
        relationship = Relationship(
            name="ORDERS_TO_PERIODS",
            from_table="ORDERS",
            from_columns=("ORDERED_AT",),
            to_table="PERIODS",
            to_columns=("START_AT",),
            range_bounds=("START_AT", "END_AT"),
        )
        assert render_relationship(relationship) == (
            "ORDERS_TO_PERIODS AS ORDERS (ORDERED_AT) " "REFERENCES PERIODS (BETWEEN START_AT AND END_AT EXCLUSIVE)"
        )

    def test_variable_default_is_rendered_verbatim(self) -> None:
        """The loader owns SQL-literal conversion, so FALSE arrives already correct."""
        var = Variable(name="TAX_INCLUSIVE", data_type="BOOLEAN", default="FALSE", comment="c")
        assert render_variable(var) == "TAX_INCLUSIVE BOOLEAN DEFAULT FALSE COMMENT = 'c'"
