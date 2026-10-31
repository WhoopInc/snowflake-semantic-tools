"""Resolve `extensions:`: the Cortex extensions a project's agents consume and it does not publish."""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.config_schema import CONFIG_FILE, config_block, target_text
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.skill import extension_identifier


def consumed_extensions(
    config: Mapping[str, object], target: TargetIdentity, *, file: str = CONFIG_FILE
) -> tuple[dict[str, QualifiedName], tuple[Diagnostic, ...]]:
    """Resolve `extensions:`: the extensions agents consume and this project does not publish.

    An entry resolves to its `fqn:`, else to `default_prefix` plus its name as an identifier;
    both may name `{{ target.* }}`.

    Returns:
        The resolved names by casefolded entry name, and one diagnostic per entry that does
        not resolve, in entry order.

    Diagnostics:
        SST-CFG036: an entry's name does not parse, the block sets no `default_prefix`, or
            the entry's key is not a single identifier.
    """
    block = config_block(config.get("extensions"))
    prefix = target_text(block.get("default_prefix"), target, None)
    resolved: dict[str, QualifiedName] = {}
    diagnostics: list[Diagnostic] = []
    for name, entry in block.items():
        if name == "default_prefix":
            continue
        fqn = target_text(entry.get("fqn"), target, None) if isinstance(entry, dict) else None
        try:
            if fqn:
                resolved[name.casefold()] = QualifiedName.parse(fqn)
                continue
            if prefix and "." not in name:
                resolved[name.casefold()] = QualifiedName.parse(f"{prefix}.{extension_identifier(name)}")
                continue
        except ValueError as exc:
            diagnostics.append(_unqualified_extension(name, f"its name does not parse ({exc})", file))
            continue
        reason = "the block sets no default_prefix" if not prefix else "the key is not a single identifier"
        diagnostics.append(_unqualified_extension(name, reason, file))
    return resolved, tuple(diagnostics)


def _unqualified_extension(name: str, reason: str, file: str) -> Diagnostic:
    return D(
        "SST-CFG036",
        origin=Origin(file),
        subject=f"config:extensions.{name}",
        block="extensions",
        name=name,
        reason=reason,
    )
