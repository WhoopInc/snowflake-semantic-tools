"""Read the values the engine uses out of a parsed `sst_config.yml` tree.

Every reader is total over what YAML can produce: a block that is absent or not a mapping
reads as empty, and a value of the wrong type reads as absent or as its text, as each
function says. The compiler, the planner, the manifest's file checksums, and the CLI read
the same keys through these functions, so they agree on what a configuration means.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from snowflake_semantic_tools.domain.model.config_schema.keys import CONFIG_KEYS
from snowflake_semantic_tools.domain.model.identifier import TargetIdentity


def config_block(value: object) -> dict[str, object]:
    """Return a block of the tree as a dict with text keys; anything but a mapping reads as empty."""
    return {str(key): item for key, item in value.items()} if isinstance(value, dict) else {}


def configured_dir(config: Mapping[str, object], key: str, default: str) -> str:
    """Return `project.<key>` as text; `default` when the block or the value is absent or empty."""
    project = config.get("project")
    return str(project.get(key) or default) if isinstance(project, dict) else default


def skills_configured(config: Mapping[str, object]) -> bool:
    """Report whether `skills:` declares a publishing channel, `catalog` or `stage`.

    Without one, skills and plugins are not compiled at all, and their folders are not part
    of the manifest's file checksums.
    """
    skills = config_block(config.get("skills"))
    return "catalog" in skills or "stage" in skills


def config_text(value: object, default: str | None) -> str | None:
    """Return a value as text with every `{{ target.* }}` template replaced by `default`.

    `{{ target.database }}`, `{{ target.schema }}` and `{{ target.warehouse }}` all become
    `default`, whichever the key names. A non-text value reads as its `str()`.

    Returns:
        The text; `default` when the value is None or the replaced text is empty.
    """
    if value is None:
        return default
    if not isinstance(value, str):
        return str(value)
    replacements = {
        "{{ target.database }}": default or "",
        "{{ target.schema }}": default or "",
        "{{ target.warehouse }}": default or "",
    }
    for raw, resolved in replacements.items():
        value = value.replace(raw, resolved)
    return value or default


def target_text(value: object, target: TargetIdentity, default: str | None) -> str | None:
    """Return a value as text with each `{{ target.* }}` template replaced by the target's own value.

    The database and schema are replaced casefolded, and the warehouse as written, or empty
    when the target sets none. A non-text value reads as its `str()`.

    Returns:
        The text; `default` when the value is None or the replaced text is empty.
    """
    if not isinstance(value, str):
        return default if value is None else str(value)
    return (
        value.replace("{{ target.database }}", target.database.folded)
        .replace("{{ target.schema }}", target.schema.folded)
        .replace("{{ target.warehouse }}", target.warehouse or "")
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
