"""
Semantic Metadata Collection Definitions

Data classes representing collections of semantic metadata extracted from
dbt YAML files and snowflake_semantic_models/ directory.

This module defines the container class that aggregates all semantic metadata
components (metrics, relationships, filters, etc.) parsed from YAML files.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class Metric:
    """
    Represents a business metric - an aggregated KPI for performance measurement.

    Metrics are quantifiable measures of business performance calculated using
    aggregate functions (SUM, AVG, COUNT) across multiple rows. They serve as
    key performance indicators (KPIs) in reports and dashboards. Unlike facts
    (row-level values), metrics are aggregated calculations that can reference
    columns from multiple tables via relationships.

    Example: total_revenue = SUM(orders.amount * (1 - orders.discount_rate))
    """

    name: str
    expression: str
    tables: List[str]
    description: Optional[str] = None
    synonyms: List[str] = field(default_factory=list)
    sample_values: List[Any] = field(default_factory=list)

    def __post_init__(self):
        """Ensure tables is always a list."""
        if isinstance(self.tables, str):
            self.tables = [self.tables]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {"name": self.name, "expr": self.expression, "tables": self.tables}

        if self.description:
            result["description"] = self.description
        if self.synonyms:
            result["synonyms"] = self.synonyms
        if self.sample_values:
            result["sample_values"] = self.sample_values

        return result


@dataclass
class Relationship:
    """
    Represents a join relationship between logical tables.

    Relationships enable cross-table analysis by defining how tables connect
    through shared keys. They allow metrics and queries to reference columns
    from multiple tables. Primary keys must be defined on tables used in
    relationships.

    Best practice: For many-to-one relationships, the left table should be
    the 'many' side and the right table should be the 'one' side.
    """

    name: str
    left_table: str
    right_table: str
    join_type: str
    relationship_type: str
    relationship_columns: List[Dict[str, str]]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "name": self.name,
            "left_table": self.left_table,
            "right_table": self.right_table,
            "join_type": self.join_type,
            "relationship_type": self.relationship_type,
            "relationship_columns": self.relationship_columns,
        }


@dataclass
class Filter:
    """
    Represents a predefined filter condition for limiting query results.

    Filters are reusable WHERE clause conditions that limit results to specific
    data subsets based on business criteria. They make it easier for users to
    apply common filtering patterns without knowing the underlying SQL.

    Example: 'last_30_days' filter with expression: "date >= CURRENT_DATE - 30"
    """

    name: str
    table_name: str
    expression: str
    description: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {"name": self.name, "table_name": self.table_name, "expression": self.expression}

        if self.description:
            result["description"] = self.description

        return result


@dataclass
class CustomInstruction:
    """
    Represents custom instructions for Cortex Analyst's behavior.

    Custom instructions provide domain-specific guidance to Cortex Analyst for
    question categorization and SQL generation. They help the AI understand
    business-specific terminology, calculation rules, and query patterns unique
    to your organization.

    Instructions can include guidance on metric calculations, date handling,
    business rules, and preferred query patterns.
    """

    name: str
    instruction: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {"name": self.name, "instruction": self.instruction}


@dataclass
class VerifiedQuery:
    """
    Represents a pre-validated query example for Cortex Analyst training.

    Verified queries are natural language questions paired with correct SQL
    answers. They serve as training examples to help Cortex Analyst understand
    how to translate business questions into accurate SQL queries. These
    validated examples improve the AI's ability to generate correct queries
    for similar questions.
    """

    name: str
    question: str
    sql: str
    tables: List[str] = field(default_factory=list)
    verified_at: Optional[str] = None
    verified_by: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {"name": self.name, "question": self.question, "sql": self.sql}

        if self.tables:
            result["tables"] = self.tables
        if self.verified_at:
            result["verified_at"] = self.verified_at
        if self.verified_by:
            result["verified_by"] = self.verified_by

        return result


@dataclass
class SemanticView:
    """
    Represents a curated semantic view for a specific business domain.

    Semantic views are curated subsets of the full semantic model, grouping
    related tables, metrics, and dimensions for specific business use cases.
    They become first-class Snowflake SEMANTIC VIEW objects that BI tools and
    Cortex Analyst can query. Each view focuses on a particular domain like
    'sales_analytics' or 'customer_360'.

    Views can include custom instructions to provide domain-specific guidance
    for query generation within their scope.
    """

    name: str
    tables: List[str]
    description: Optional[str] = None
    custom_instructions: List[str] = field(default_factory=list)

    def __post_init__(self):
        """Ensure tables is always a list."""
        if isinstance(self.tables, str):
            self.tables = [self.tables]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {"name": self.name, "tables": self.tables}

        if self.description:
            result["description"] = self.description
        if self.custom_instructions:
            result["custom_instructions"] = self.custom_instructions

        return result


@dataclass
class SemanticMetadataCollection:
    """
    Container for all semantic metadata parsed from YAML files.

    This class aggregates semantic model components extracted from:
    - snowflake_semantic_models/metrics/*.yml
    - snowflake_semantic_models/relationships/*.yml
    - snowflake_semantic_models/filters/*.yml
    - snowflake_semantic_models/custom_instructions/*.yml
    - snowflake_semantic_models/verified_queries/*.yml
    - snowflake_semantic_models/semantic_views.yml

    It serves as an in-memory representation of the complete semantic layer
    configuration before being loaded into Snowflake metadata tables or used
    for validation and generation operations.

    Components work together to define:
    - Metrics: Aggregated KPIs and business calculations
    - Relationships: How tables join together
    - Filters: Reusable WHERE clause conditions
    - Custom Instructions: AI guidance for query generation
    - Verified Queries: Pre-validated example queries
    - Semantic Views: Curated subsets for specific domains
    """

    metrics: List[Metric] = field(default_factory=list)
    relationships: List[Relationship] = field(default_factory=list)
    filters: List[Filter] = field(default_factory=list)
    custom_instructions: List[CustomInstruction] = field(default_factory=list)
    verified_queries: List[VerifiedQuery] = field(default_factory=list)
    semantic_views: List[SemanticView] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {}

        if self.metrics:
            result["metrics"] = [m.to_dict() for m in self.metrics]
        if self.relationships:
            result["relationships"] = [r.to_dict() for r in self.relationships]
        if self.filters:
            result["filters"] = [f.to_dict() for f in self.filters]
        if self.custom_instructions:
            result["custom_instructions"] = [c.to_dict() for c in self.custom_instructions]
        if self.verified_queries:
            result["verified_queries"] = [v.to_dict() for v in self.verified_queries]
        if self.semantic_views:
            result["semantic_views"] = [s.to_dict() for s in self.semantic_views]

        return result

    def merge(self, other: "SemanticMetadataCollection") -> "SemanticMetadataCollection":
        """
        Merge another semantic metadata collection into this one.

        Args:
            other: Another semantic metadata collection to merge

        Returns:
            New semantic metadata collection with combined data
        """
        return SemanticMetadataCollection(
            metrics=self.metrics + other.metrics,
            relationships=self.relationships + other.relationships,
            filters=self.filters + other.filters,
            custom_instructions=self.custom_instructions + other.custom_instructions,
            verified_queries=self.verified_queries + other.verified_queries,
            semantic_views=self.semantic_views + other.semantic_views,
        )
