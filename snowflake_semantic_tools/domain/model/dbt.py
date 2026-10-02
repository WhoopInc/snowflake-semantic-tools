"""Immutable projections of what SST consumes from dbt: manifest models and the resolved target."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DbtTarget:
    """The resolved dbt target -- what `{{ target.database }}` stands for."""

    database: str
    schema: str

    def fqn(self, name: str) -> str:
        """`name`, upper-cased, qualified by this target's database and schema."""
        return f"{self.database}.{self.schema}.{name.upper()}"


@dataclass(frozen=True, slots=True)
class DbtColumn:
    """One dbt model column after dbt has resolved patches and inheritance."""

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
    # `meta.sst.access_modifier` as written; None when the column writes none.
    access_modifier: str | None = None


@dataclass(frozen=True, slots=True)
class DbtModel:
    """The manifest-backed relation and semantic metadata for one dbt model."""

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
    # Whether dbt enforces a contract on the model, and whether any test is attached to it.
    contract_enforced: bool = False
    tested: bool = False

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
class DbtCatalog:
    """All dbt models visible to one parsed target."""

    schema_version: str
    dbt_version: str | None
    project_name: str | None
    models: tuple[DbtModel, ...]
    # Models left out because they have no relation and no SST metadata, such as ephemeral ones.
    relationless_models: tuple[str, ...] = ()
    # Models dbt lists as disabled, which produce no relation either.
    disabled_models: tuple[str, ...] = ()

    def unavailable_models(self) -> dict[str, str]:
        """Each model that produces no relation, by casefolded name, with why: `disabled` or `ephemeral`."""
        found = {name.casefold(): "ephemeral" for name in self.relationless_models}
        found.update((name.casefold(), "disabled") for name in self.disabled_models)
        return found

    def model(self, name: str) -> DbtModel | None:
        """Return a model by its dbt logical name, case-insensitively."""
        wanted = name.casefold()
        return next((model for model in self.models if model.name.casefold() == wanted), None)
