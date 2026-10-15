"""Name the tags `sst_config.yml` declares, and resolve a `{{ tag('<name>') }}` reference to one.

Views and agents both name a tag object by the key it has under the top-level `tags:` block,
never by its object name, so a target's database and schema follow from the configuration.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.config_schema import CONFIG_FILE, target_text
from snowflake_semantic_tools.domain.model.dbt import DbtTarget
from snowflake_semantic_tools.domain.model.identifier import TargetIdentity
from snowflake_semantic_tools.domain.resolve.calls import required_call
from snowflake_semantic_tools.domain.resolve.ref_fields import reference_functions


@dataclass(frozen=True, slots=True)
class DeclaredTags:
    """The tags the `tags:` block declares, by their key there.

    Attributes:
        names: Each key that can be qualified, to its object name.
        unqualified: Each key that cannot, to the SST-CFG036 that says why.
    """

    names: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    unqualified: Mapping[str, Diagnostic] = field(default_factory=lambda: MappingProxyType({}))


def declared_tags(block: object, target: TargetIdentity | DbtTarget) -> DeclaredTags:
    """Name each tag `tags:` declares: its `fqn:`, else `default_prefix` and its upper-cased key.

    Both may name `{{ target.database }}` and `{{ target.schema }}`. A block that is not a
    mapping declares no tag.

    Diagnostics:
        SST-CFG036: an entry has no `fqn:`, and the block sets no `default_prefix` or the
            entry's key is dotted, which would render a four-part name.
    """
    if not isinstance(block, Mapping):
        return DeclaredTags()
    prefix = target_text(block.get("default_prefix"), target, "") or ""
    names: dict[str, str] = {}
    unqualified: dict[str, Diagnostic] = {}
    for key, entry in block.items():
        name = str(key)
        if name == "default_prefix":
            continue
        fqn = (target_text(entry.get("fqn"), target, "") or "") if isinstance(entry, Mapping) else ""
        if fqn:
            names[name] = fqn
        elif prefix and "." not in name:
            names[name] = f"{prefix}.{name.upper()}"
        else:
            reason = "the block sets no default_prefix" if not prefix else "the key is not a single identifier"
            unqualified[name] = D(
                "SST-CFG036",
                origin=Origin(CONFIG_FILE),
                subject=f"config:tags.{name}",
                block="tags",
                name=name,
                reason=reason,
            )
    return DeclaredTags(MappingProxyType(names), MappingProxyType(unqualified))


def tag_reference(
    text: str,
    tags: DeclaredTags,
    origin: Origin | None,
    *,
    reference: str,
    field: str,
    artifact: str,
    subject: str | None = None,
) -> str | Diagnostic:
    """Resolve a tag's `name:`, one `{{ tag('<name>') }}` call, to the object name it declares.

    Args:
        reference: The field's entry in `REFERENCE_FIELDS`.

    Diagnostics:
        Every code `required_call` lists, for a name that is not one `tag()` call.
        SST-CFG036: the call names a declared tag that cannot be qualified.
        SST-REF028: the call names no declared tag.
    """
    found = required_call(text, reference_functions(reference), origin, field=field, artifact=artifact, subject=subject)
    if isinstance(found, Diagnostic):
        return found
    name = found.args[0]
    if name in tags.unqualified:
        return tags.unqualified[name]
    if name not in tags.names:
        return D("SST-REF028", origin=origin, subject=subject, name=name)
    return tags.names[name]
