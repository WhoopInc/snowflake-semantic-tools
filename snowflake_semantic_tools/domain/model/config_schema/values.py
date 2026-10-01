"""Read the values the engine uses out of a parsed `sst_config.yml` tree.

Every reader is total over what YAML can produce: a block that is absent or not a mapping
reads as empty, and a value of the wrong type reads as absent or as its text, as each
function says. The compiler, the planner, the manifest's file checksums, and the CLI read
the same keys through these functions, so they agree on what a configuration means.
"""

from __future__ import annotations

from typing import Mapping

from ..identifier import TargetIdentity


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
