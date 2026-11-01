"""Immutable projections of what SST consumes from dbt: manifest models and the resolved target."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.identifier import Identifier


@dataclass(frozen=True, slots=True)
class DbtTarget:
    """The resolved dbt target -- what `{{ target.database }}` stands for.

    Attributes:
        database, schema: The target's database and schema, as the dbt profile resolves them.
    """

    database: str
    schema: str

    def fqn(self, name: str) -> str:
        """`name`, upper-cased, qualified by this target's database and schema."""
        return f"{self.database}.{self.schema}.{name.upper()}"


@dataclass(frozen=True, slots=True)
class DbtColumn:
    """One dbt model column after dbt has resolved patches and inheritance.

    A quoted column keeps its exact case wherever SST names it; any other is upper-cased.

    Authored keys: `authored_schema.DBT_COLUMN_META`.

    Attributes:
        name: The column's name as the manifest records it.
        description: The column's description, stripped; None when empty.
        data_type: dbt's own `data_type`, else `meta.sst.data_type`; None when neither is written.
        column_type: `meta.sst.column_type` as written, such as `fact` or `dimension`; None when unset.
        synonyms: `meta.sst.synonyms`; empty when none.
        sample_values: `meta.sst.sample_values`; empty when none.
        is_enum: `meta.sst.is_enum`; None when the column declares none, so nobody has said whether its
            values are complete.
        excluded: `meta.sst.exclude`: the column is left out of every view; False when unset.
        unknown_meta_keys: The `meta.sst` keys SST does not read, sorted; reported where a view uses the
            model.
        declared_data_type: A `meta.sst.data_type` that disagrees with dbt's own; None when they agree or
            either is absent.
        declared_keys: The `meta.sst` keys the column writes, so an absent key and an empty value differ.
        native_data_type: dbt's own `data_type`, which a contract enforces; None when only
            `meta.sst.data_type` is written.
        pii_tagged: Whether the column carries `pii_tags` in its meta, of any privacy category.
        governance_tags: `meta.sst.governance_tags` as written: the sensitive tags enrich recorded.
        access_modifier: `meta.sst.access_modifier` as written; None when the column writes none.
        quoted: dbt's `quote: true`: the column was created quoted, so its name is case-sensitive.
        wrong_types: `meta.sst` values of the wrong type, as `(field, expected, found)`; each reads as
            unset and is reported where a view uses the model.
        unstorable: Published text that holds what Snowflake cannot store, as `(field, problem)`.
    """

    name: str
    description: str | None
    data_type: str | None
    column_type: str | None
    synonyms: tuple[str, ...] = ()
    sample_values: tuple[str, ...] = ()
    # None when the column declares no `is_enum`: nobody has said whether its values are complete.
    is_enum: bool | None = None
    excluded: bool = False
    # `meta.sst` keys SST does not read, reported where a view uses the model.
    unknown_meta_keys: tuple[str, ...] = ()
    # A meta.sst.data_type that disagrees with dbt's own; `data_type` holds dbt's.
    declared_data_type: str | None = None
    # The `meta.sst` keys the column writes, so an absent key and an empty value differ.
    declared_keys: frozenset[str] = frozenset()
    # dbt's own `data_type`, which a contract enforces; None when only meta.sst.data_type is written.
    native_data_type: str | None = None
    # The column carries `pii_tags` in its meta, of any privacy category.
    pii_tagged: bool = False
    # `meta.sst.governance_tags` as written: the sensitive Snowflake tags enrich recorded.
    governance_tags: tuple[str, ...] = ()
    # `meta.sst.access_modifier` as written; None when the column writes none.
    access_modifier: str | None = None
    # dbt's `quote: true`: the column was created quoted, so its name is case-sensitive.
    quoted: bool = False
    # `meta.sst` values of the wrong type, as `(field, expected, found)`: each reads as unset, and
    # is reported where a view uses the model rather than read by Python's truthiness.
    wrong_types: tuple[tuple[str, str, str], ...] = ()
    # Published text that holds what Snowflake cannot store, as `(field, problem)`.
    unstorable: tuple[tuple[str, str], ...] = ()

    @property
    def identifier(self) -> Identifier:
        """The column as Snowflake names it: its exact name when quoted, else upper-cased."""
        return Identifier(self.name, quoted=self.quoted)

    @property
    def sensitive(self) -> bool:
        """Report whether the YAML marks the column sensitive: `pii_tags`, or a recorded governance tag."""
        return self.pii_tagged or bool(self.governance_tags)

    @property
    def member_name(self) -> str:
        """The column's name as a view member holds it: double-quoted when quoted, else as written."""
        return self.identifier.sql if self.quoted else self.name


@dataclass(frozen=True, slots=True)
class DbtModel:
    """The manifest-backed relation and semantic metadata for one dbt model, snapshot or seed.

    All three are relations `ref()` resolves, so a view uses any of them as a table alike;
    `resource_type` says which it is.

    Authored keys: `authored_schema.DBT_MODEL_META`.

    Attributes:
        unique_id: dbt's id for the node, such as `model.jaffle.orders`.
        name: The model's dbt name.
        relation_name: The relation dbt builds, stripped and upper-cased.
        primary_key: `config.meta.sst.primary_key`'s columns; empty when the model declares none.
        unique_keys: `config.meta.sst.unique_keys`, one column tuple per key; empty when none.
        columns: The model's columns, in manifest order.
        original_file_path: The model's SQL file as the manifest records it; None when absent.
        patch_path: The YAML file that patches the model, with dbt's `<package>://` prefix; None when
            absent.
        forbidden_location_keys: Which of `database` and `schema` the model writes in `meta.sst`, where
            they are not allowed; empty when neither.
        description: The model's description, stripped; None when empty.
        legacy_key_fields: Key fields written in a 0.3 form; they are reported, not read.
        unknown_meta_keys: The `meta.sst` keys SST does not read, sorted; reported where a view uses the
            model.
        key_test_columns: Casefolded columns a `unique`, `unique_combination_of_columns` or
            `relationships` test names.
        package_name: The dbt package that defines the model; None when the manifest does not say.
        raw_relation_name: `relation_name` exactly as dbt rendered it, quotes and case included.
        patch_file: `patch_path` relative to the project root, without dbt's `<package>://` prefix.
        checksum: dbt's `checksum.checksum` of the model's SQL file; None when the manifest records none.
        has_contract: Whether the model declares an enforced contract.
        test_count: How many dbt tests are attached to the model.
        materialized: `config.materialized`, such as `table` or `incremental`; None when the manifest
            does not say.
        synonyms: `meta.sst.synonyms`: what every view that uses the model calls its table besides its
            name.
        distinct_range: `meta.sst.distinct_range` as its `(start, end)` columns; None when the model
            declares none, or one that is not a mapping naming both.
        malformed_distinct_range: The model writes `meta.sst.distinct_range`, but not as a mapping naming
            a start and an end.
        wrong_types, unstorable: As `DbtColumn.wrong_types` and `DbtColumn.unstorable`, for the model's
            own fields.
        resource_type: dbt's `resource_type`: `model`, `snapshot` or `seed`.
    """

    unique_id: str
    name: str
    relation_name: str
    primary_key: tuple[str, ...]
    unique_keys: tuple[tuple[str, ...], ...]
    columns: tuple[DbtColumn, ...]
    original_file_path: str | None = None
    patch_path: str | None = None
    forbidden_location_keys: tuple[str, ...] = ()
    description: str | None = None
    # Key fields written in a 0.3 form. They are reported, not read.
    legacy_key_fields: tuple[str, ...] = ()
    # `meta.sst` keys SST does not read, reported where a view uses the model.
    unknown_meta_keys: tuple[str, ...] = ()
    # Casefolded columns a `unique`, `unique_combination_of_columns` or `relationships` test names.
    key_test_columns: frozenset[str] = frozenset()
    # The dbt package that defines the model; None when the manifest does not say.
    package_name: str | None = None
    # `relation_name` exactly as dbt rendered it, quotes and case included.
    raw_relation_name: str | None = None
    # `patch_path` relative to the project root, without dbt's `<package>://` prefix.
    patch_file: str | None = None
    # dbt's `checksum.checksum` of the model's SQL file; None when the manifest records none.
    checksum: str | None = None
    # The model declares an enforced contract.
    has_contract: bool = False
    # How many dbt tests are attached to the model.
    test_count: int = 0
    # `config.materialized`, such as `table` or `incremental`; None when the manifest does not say.
    materialized: str | None = None
    # `meta.sst.synonyms`: what every view that uses the model calls its table besides its name.
    synonyms: tuple[str, ...] = ()
    # `meta.sst.distinct_range` as its `(start, end)` columns; None when the model declares none,
    # or declares one that is not a mapping naming both.
    distinct_range: tuple[str, str] | None = None
    # The model writes `meta.sst.distinct_range`, but not as a mapping naming a start and an end.
    malformed_distinct_range: bool = False
    # As `DbtColumn.wrong_types` and `DbtColumn.unstorable`, for the model's own fields.
    wrong_types: tuple[tuple[str, str, str], ...] = ()
    unstorable: tuple[tuple[str, str], ...] = ()
    # dbt's `resource_type`: `model`, `snapshot` or `seed`.
    resource_type: str = "model"

    def column(self, name: str) -> DbtColumn | None:
        """Return a column case-insensitively."""
        wanted = name.casefold()
        return next((column for column in self.columns if column.name.casefold() == wanted), None)

    def is_key_column(self, name: str) -> bool:
        """Report whether a column is declared a key, or is named by a key or relationship test."""
        wanted = name.casefold()
        declared = (*self.primary_key, *(column for key in self.unique_keys for column in key))
        return wanted in self.key_test_columns or any(column.casefold() == wanted for column in declared)


@dataclass(frozen=True, slots=True)
class DbtSource:
    """One dbt source table: `source('<source_name>', '<name>')`, and the relation it reads.

    Attributes:
        unique_id: dbt's id for the source table, such as `source.jaffle.raw.orders`.
        source_name, name: The source's name and the table's name, as `source()` takes them.
        relation_name: The relation it reads, as dbt rendered it; None when the manifest records none.
        description: The source table's `description`, the table's COMMENT in every view that uses it.
        synonyms: The source table's `meta.sst.synonyms`; empty when none.
        package: The dbt package that declares it; None when the manifest does not say.
    """

    unique_id: str
    source_name: str
    name: str
    relation_name: str | None
    # The source table's `description`, the table's COMMENT in every view that uses it.
    description: str | None = None
    # The source table's `meta.sst.synonyms`.
    synonyms: tuple[str, ...] = ()
    # The dbt package that declares it; None when the manifest does not say.
    package: str | None = None


@dataclass(frozen=True, slots=True)
class DbtNode:
    """One node of dbt's graph, as lineage walks it: a model, seed, snapshot, source, test or exposure.

    Attributes:
        unique_id: dbt's id, such as `model.jaffle.orders` or `source.jaffle.raw.orders`.
        kind: `model`, `seed`, `snapshot`, `source`, `test` or `exposure`.
        name: The model's, seed's, snapshot's, test's or exposure's name; a source's
            `<source>.<table>`.
        package: The dbt package that defines it; None when the manifest does not say.
        relation: The relation it builds or reads, as dbt renders it upper-cased; None for a
            test, an exposure, or a model with no relation.
        depends_on: The unique ids of the nodes it reads, sorted.
    """

    unique_id: str
    kind: str
    name: str
    package: str | None = None
    relation: str | None = None
    depends_on: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DbtCatalog:
    """All dbt models visible to one parsed target.

    Attributes:
        schema_version: The manifest's `dbt_schema_version`; empty when it records none.
        dbt_version: The dbt version that wrote the manifest; None when it does not say.
        project_name: The root dbt project's name; None when the manifest does not say.
        models: The models, snapshots and seeds that have a relation, in unique-id order.
        relationless_models: Models left out because they have no relation, such as ephemeral ones.
        unreadable_models: Why each model with no relation has none, by casefolded name: its
            materialization, such as `ephemeral`, or `disabled` for a model dbt disabled.
        sources: The declared dbt sources, in sorted unique-id order.
        nodes: dbt's graph: every model, seed, snapshot, source, test and exposure, in unique-id order.
        diagnostics: What reading the manifest found wrong with nodes SST skipped or with the project.
    """

    schema_version: str
    dbt_version: str | None
    project_name: str | None
    models: tuple[DbtModel, ...]
    # Models left out because they have no relation, such as ephemeral ones.
    relationless_models: tuple[str, ...] = ()
    # Why each model with no relation has none, by casefolded name: its materialization, such
    # as `ephemeral`, or `disabled` for a model dbt moved to the manifest's `disabled` map.
    unreadable_models: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    # The declared dbt sources, in sorted unique-id order.
    sources: tuple[DbtSource, ...] = ()
    # dbt's graph: every model, seed, snapshot, source, test and exposure, in unique-id order.
    nodes: tuple[DbtNode, ...] = ()
    # What reading the manifest found wrong with nodes SST skipped or with the project as a whole.
    diagnostics: tuple[Diagnostic, ...] = ()

    def source(self, source_name: str, name: str) -> DbtSource | None:
        """Return the declaration `source('<source_name>', '<name>')` reads, as dbt resolves it.

        Names compare casefolded, and only a declaration with a relation counts. When the pair is
        declared more than once, the root project's declaration wins, as it does in dbt for a
        call the root project makes; otherwise the first in unique-id order, which SST-DBT012
        reports as ambiguous.
        """
        pair = f"{source_name}.{name}".casefold()
        found = [
            item for item in self.sources if item.relation_name and f"{item.source_name}.{item.name}".casefold() == pair
        ]
        own = [item for item in found if self.project_name is not None and item.package == self.project_name]
        chosen = own or found
        return chosen[0] if chosen else None

    def model(self, name: str) -> DbtModel | None:
        """Return a model by its dbt logical name, case-insensitively."""
        wanted = name.casefold()
        return next((model for model in self.models if model.name.casefold() == wanted), None)
