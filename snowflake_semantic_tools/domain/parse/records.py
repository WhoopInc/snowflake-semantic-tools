"""Check a parsed document's root keys against the registry, and a parser's records for immutability."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import is_dataclass

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.registry import Registry


def registered_root_keys(registry: Registry) -> frozenset[str]:
    """Every YAML root key some artifact or member type of the registry owns."""
    artifacts = (artifact.root_key for artifact in registry.artifacts.values() if artifact.root_key is not None)
    return frozenset((*artifacts, *(member.root_key for member in registry.members.values())))


def root_key_diagnostics(
    path: str, root_keys: Iterable[str], known: frozenset[str], line_of: Callable[[str], int | None]
) -> tuple[Diagnostic, ...]:
    """Report a document whose root keys no registered type owns.

    Args:
        line_of: The line a root key is written on, when the document records it.

    Diagnostics:
        SST-LOD021: the document has root keys and none is registered.
        SST-PRS001: otherwise, once per root key no type owns, in document order.
    """
    keys = tuple(root_keys)
    unknown = tuple(key for key in keys if key not in known)
    if keys and len(unknown) == len(keys):
        return (D("SST-LOD021", origin=Origin(path), file=path),)
    return tuple(D("SST-PRS001", origin=Origin(path, line_of(key)), key=key) for key in unknown)


def is_frozen_record(value: object) -> bool:
    """Report whether a parsed record is immutable: a frozen dataclass, or a tuple of frozen records."""
    if isinstance(value, tuple):
        return all(is_frozen_record(item) for item in value)
    if not is_dataclass(value) or isinstance(value, type):
        return False
    params = getattr(type(value), "__dataclass_params__", None)
    return bool(getattr(params, "frozen", False))


def mutable_records(type_name: str, records: Iterable[object]) -> tuple[Diagnostic, ...]:
    """Report each record a parser returned that the model layer cannot treat as immutable.

    Diagnostics:
        SST-PRS900: a record is not a frozen dataclass, nor a tuple of them; once per class.
    """
    classes = sorted({type(record).__name__ for record in records if not is_frozen_record(record)})
    return tuple(D("SST-PRS900", type=type_name, cls=name) for name in classes)
