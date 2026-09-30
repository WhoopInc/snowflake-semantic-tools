"""Immutable projection of the dbt manifest fields SST consumes."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DbtColumn:
    """One dbt model column after dbt has resolved patches and inheritance."""

    name: str
    description: str | None
    data_type: str | None
    column_type: str | None
    synonyms: tuple[str, ...] = ()
    sample_values: tuple[str, ...] = ()
    is_enum: bool = False
    excluded: bool = False
    # `meta.sst` keys SST does not read, reported where a view uses the model.
    unknown_meta_keys: tuple[str, ...] = ()
    # A meta.sst.data_type that disagrees with dbt's own; `data_type` holds dbt's.
    declared_data_type: str | None = None


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

    def column(self, name: str) -> DbtColumn | None:
        """Return a column case-insensitively."""
        wanted = name.casefold()
        return next((column for column in self.columns if column.name.casefold() == wanted), None)


@dataclass(frozen=True, slots=True)
class DbtCatalog:
    """All dbt models visible to one parsed target."""

    schema_version: str
    dbt_version: str | None
    project_name: str | None
    models: tuple[DbtModel, ...]

    def model(self, name: str) -> DbtModel | None:
        """Return a model by its dbt logical name, case-insensitively."""
        wanted = name.casefold()
        return next((model for model in self.models if model.name.casefold() == wanted), None)
