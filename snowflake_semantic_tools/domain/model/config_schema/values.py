"""Read the values the engine uses out of a parsed `sst_config.yml` tree.

Every reader is total over what YAML can produce: a block that is absent or not a mapping
reads as empty, and a value of the wrong type reads as absent or as its text, as each
function says. The compiler, the planner, the manifest's file checksums, and the CLI read
the same keys through these functions, so they agree on what a configuration means.
"""

from __future__ import annotations

import posixpath
from collections.abc import Mapping
from dataclasses import dataclass

from snowflake_semantic_tools.domain.model.config_schema.keys import CONFIG_KEYS
from snowflake_semantic_tools.domain.model.dbt import DbtTarget
from snowflake_semantic_tools.domain.model.governance import TagPattern
from snowflake_semantic_tools.domain.model.identifier import TargetIdentity
from snowflake_semantic_tools.domain.model.registry import ArtifactType


def config_block(value: object) -> dict[str, object]:
    """Return a block of the tree as a dict with text keys; anything but a mapping reads as empty."""
    return {str(key): item for key, item in value.items()} if isinstance(value, Mapping) else {}


def stated(block: object, key: str) -> bool:
    """Report whether a block states `key`: present with a value, as `key:` written empty is unset."""
    return isinstance(block, Mapping) and block.get(key) is not None


def configured_dir(config: Mapping[str, object], key: str, default: str) -> str:
    """Return `project.<key>` as text; `default` when the block or the value is absent or empty."""
    project = config.get("project")
    return str(project.get(key) or default) if isinstance(project, dict) else default


def route_root(config: Mapping[str, object], artifact: ArtifactType) -> str:
    """Return the directory an artifact type's folder routes start from, relative to the project root.

    It is the type's `project.<dir_key>`, else that key's default, then its `route_dir` below it;
    an artifact's folder, which its routes fold along, is its directory below this one.
    """
    default = CONFIG_KEYS[f"project.{artifact.dir_key}"].default or artifact.dir_key
    directory = configured_dir(config, artifact.dir_key, default)
    return posixpath.join(directory, artifact.route_dir) if artifact.route_dir else directory


def target_text(value: object, target: TargetIdentity | DbtTarget, default: str | None) -> str | None:
    """Return a value as text with each `{{ target.* }}` template replaced by the target's own value.

    This is the one renderer of `{{ target.database }}`, `{{ target.schema }}` and
    `{{ target.warehouse }}`, each replaced by the value of the key it names. A profile target's
    database and schema are replaced casefolded and its warehouse as written, or empty when it
    sets none; a dbt target's database and schema are replaced as dbt resolved them, and it has
    no warehouse. A non-text value reads as its `str()`.

    Returns:
        The text; `default` when the value is None or the replaced text is empty.
    """
    if not isinstance(value, str):
        return default if value is None else str(value)
    if isinstance(target, DbtTarget):
        database, schema, warehouse = target.database, target.schema, ""
    else:
        database, schema, warehouse = target.database.folded, target.schema.folded, target.warehouse or ""
    return (
        value.replace("{{ target.database }}", database)
        .replace("{{ target.schema }}", schema)
        .replace("{{ target.warehouse }}", warehouse)
        or default
    )


def config_int(value: object) -> int | None:
    """Return an integer value; None for anything else, a boolean included."""
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def config_bool(value: object) -> bool | None:
    """Return a boolean value; None for anything else."""
    return value if isinstance(value, bool) else None


def _enrichment_default(name: str) -> str:
    return CONFIG_KEYS[f"enrichment.{name}"].default or ""


@dataclass(frozen=True, slots=True)
class DbtSettings:
    """The `dbt:` block: how SST produces and reads the dbt manifest, each key at its default when unset.

    Attributes:
        invoke: Whether SST runs dbt before reading the manifest; false reads what is on disk.
        command: The dbt command that writes the manifest: `parse`, or `compile`.
        manifest_schema_versions: The manifest schema versions accepted; None accepts the ones
            this release is tested against.
    """

    invoke: bool = True
    command: str = "parse"
    manifest_schema_versions: frozenset[int] | None = None


def dbt_settings(config: Mapping[str, object]) -> DbtSettings:
    """Read the `dbt:` block; a key that is absent or of the wrong type reads as its default."""
    block = config_block(config.get("dbt"))
    invoke = config_bool(block.get("invoke"))
    command = block.get("command")
    versions = block.get("manifest_schema_versions")
    numbers = (
        frozenset(item for item in versions if isinstance(item, int) and not isinstance(item, bool))
        if isinstance(versions, list)
        else None
    )
    return DbtSettings(
        invoke=True if invoke is None else invoke,
        command=command if isinstance(command, str) and command in ("parse", "compile") else "parse",
        manifest_schema_versions=numbers or None,
    )


@dataclass(frozen=True, slots=True)
class EnrichmentConfig:
    """The `enrichment:` block as `sst enrich` reads it, each key at its default when unset.

    Attributes:
        distinct_limit: Distinct values sampled per column; no more than this many is an enum.
        display_limit: Sample values written for a column that is not an enum.
        synonym_model: The Cortex model that writes synonyms.
        synonym_max_count: Synonyms written per column and per table.
        allow_sample_value_collection: False refuses every run that reads row data.
    """

    distinct_limit: int = int(_enrichment_default("distinct_limit"))
    display_limit: int = int(_enrichment_default("sample_values_display_limit"))
    synonym_model: str = _enrichment_default("synonym_model")
    synonym_max_count: int = int(_enrichment_default("synonym_max_count"))
    allow_sample_value_collection: bool = _enrichment_default("allow_sample_value_collection") == "true"


def enrichment_config(config: Mapping[str, object]) -> EnrichmentConfig:
    """Read the `enrichment:` block; a key that is absent or of the wrong type reads as its default.

    An integer below 1 reads as its default too: validation reports it, and no limit can be zero.
    """
    block = config_block(config.get("enrichment"))
    defaults = EnrichmentConfig()

    def limit(name: str, default: int) -> int:
        value = config_int(block.get(name))
        return value if value is not None and value >= 1 else default

    model = block.get("synonym_model")
    allowed = config_bool(block.get("allow_sample_value_collection"))
    return EnrichmentConfig(
        distinct_limit=limit("distinct_limit", defaults.distinct_limit),
        display_limit=limit("sample_values_display_limit", defaults.display_limit),
        synonym_model=model.strip() if isinstance(model, str) and model.strip() else defaults.synonym_model,
        synonym_max_count=limit("synonym_max_count", defaults.synonym_max_count),
        allow_sample_value_collection=defaults.allow_sample_value_collection if allowed is None else allowed,
    )


@dataclass(frozen=True, slots=True)
class GovernanceConfig:
    """The `governance:` block, each key at its default when unset.

    Attributes:
        sensitive_tags: The entries that parse, in order; one that does not is left out, and
            validation refuses it (SST-CFG057).
        read_column_tags: Whether enrich and the connected validate read column tags.
    """

    sensitive_tags: tuple[TagPattern, ...] = ()
    read_column_tags: bool = False

    @property
    def reads_tags(self) -> bool:
        """Report whether a run reads column tags: reading is on, and some tag is named sensitive."""
        return self.read_column_tags and bool(self.sensitive_tags)


def tag_pattern(entry: object) -> TagPattern | None:
    """Return a `governance.sensitive_tags` entry as the tags it names; None when it is no tag name."""
    if not isinstance(entry, str):
        return None
    try:
        return TagPattern.parse(entry)
    except ValueError:
        return None


def governance_config(config: Mapping[str, object]) -> GovernanceConfig:
    """Read the `governance:` block; a key that is absent or of the wrong type reads as its default."""
    block = config_block(config.get("governance"))
    entries = block.get("sensitive_tags")
    patterns = (tag_pattern(entry) for entry in (entries if isinstance(entries, list) else ()))
    return GovernanceConfig(
        tuple(pattern for pattern in patterns if pattern is not None),
        config_bool(block.get("read_column_tags")) is True,
    )
