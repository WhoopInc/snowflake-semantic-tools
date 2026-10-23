"""The parts of dbt's `manifest.json` SST reads, as msgspec structs, for a decode that skips the rest.

A large manifest is mostly compiled SQL, macros, docs and parent maps SST never reads; decoding
into these structs skips them. Every field SST reads is kept as decoded JSON (`Any`, `UNSET` when
absent), so `manifest.catalog_from_document` sees the same values, types, and missing-versus-null
distinctions it sees in the plain decode: only keys it never reads are gone. `meta` is kept whole,
because the keys of `meta.sst` are read, not only its values.

The keys, as `manifest.py` reads them:

- the root: `metadata`, `nodes`, `sources`, `exposures`, `disabled`;
- `metadata`: `dbt_schema_version`, `dbt_version`, `project_name`;
- a node, source or exposure: `resource_type`, `name`, `source_name`, `relation_name`, `database`,
  `schema`, `description`, `original_file_path`, `patch_path`, `package_name`, `checksum`,
  `contract`, `config`, `meta`, `columns`, `test_metadata`, `attached_node`, `column_name`,
  `depends_on`;
- a node's `config`: `materialized`, `meta`, `contract`;
- a column: `name`, `description`, `data_type`, `quote`, `meta`, `config`.

A key `manifest.py` starts reading must be added here, or the fast path would drop it;
`tests/unit/adapters/test_manifest_schema.py` compares both decodes on real manifests.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import msgspec

from snowflake_semantic_tools.adapters.json_files import MAX_JSON_DEPTH

_UNSET: Any = msgspec.UNSET


class _Column(msgspec.Struct):
    name: Any = _UNSET
    description: Any = _UNSET
    data_type: Any = _UNSET
    quote: Any = _UNSET
    meta: Any = _UNSET
    config: Any = _UNSET


class _Config(msgspec.Struct):
    materialized: Any = _UNSET
    meta: Any = _UNSET
    contract: Any = _UNSET


class _Node(msgspec.Struct):
    resource_type: Any = _UNSET
    name: Any = _UNSET
    source_name: Any = _UNSET
    relation_name: Any = _UNSET
    database: Any = _UNSET
    schema: Any = _UNSET
    description: Any = _UNSET
    original_file_path: Any = _UNSET
    patch_path: Any = _UNSET
    package_name: Any = _UNSET
    checksum: Any = _UNSET
    contract: Any = _UNSET
    config: _Config | None | msgspec.UnsetType = _UNSET
    meta: Any = _UNSET
    columns: dict[str, _Column] | None | msgspec.UnsetType = _UNSET
    test_metadata: Any = _UNSET
    attached_node: Any = _UNSET
    column_name: Any = _UNSET
    depends_on: Any = _UNSET


class _Metadata(msgspec.Struct):
    dbt_schema_version: Any = _UNSET
    dbt_version: Any = _UNSET
    project_name: Any = _UNSET


class _Manifest(msgspec.Struct):
    metadata: _Metadata | msgspec.UnsetType = _UNSET
    nodes: dict[str, _Node] | msgspec.UnsetType = _UNSET
    sources: dict[str, _Node] | msgspec.UnsetType = _UNSET
    exposures: dict[str, _Node] | msgspec.UnsetType = _UNSET
    disabled: dict[str, list[_Node]] | msgspec.UnsetType = _UNSET


_DECODER = msgspec.json.Decoder(_Manifest)


def decode_manifest(raw: bytes) -> dict[str, Any] | None:
    """Decode the manifest bytes into plain JSON values with only the keys SST reads, or None.

    None means the fast decode does not read this document the way the plain decode would: it is
    not UTF-8 JSON, a part the structs type has another shape (a node that is not an object, a
    `columns` that is a list), it uses a value msgspec refuses and `json` accepts (`NaN`), or a
    kept part nests past `MAX_JSON_DEPTH`. The caller then reads it the plain way, which reports
    what it reports today.
    """
    try:
        manifest = _DECODER.decode(raw)
    except (msgspec.DecodeError, RecursionError):
        # ValidationError is a DecodeError: a part of another shape lands here too.
        return None
    if any(_deeper(value, MAX_JSON_DEPTH - depth) for depth, value in _free_values(manifest)):
        return None
    return msgspec.to_builtins(manifest)  # type: ignore[no-any-return]


def _free_values(manifest: _Manifest) -> Iterator[tuple[int, dict[Any, Any] | list[Any]]]:
    """Each kept value of free shape that holds an array or object, with the levels above it.

    The structs fix how deep their own layers sit, so only these can nest past the bound; checking
    them alone keeps the bound without walking every column of a large manifest.
    """
    for depth, struct in _structs(manifest):
        for field in struct.__struct_fields__:
            value = getattr(struct, field)
            if isinstance(value, (dict, list)) and not isinstance(value, msgspec.Struct):
                # The struct itself is one more level around the value.
                yield depth + 1, value


def _structs(manifest: _Manifest) -> Iterator[tuple[int, msgspec.Struct]]:
    """Each struct the manifest decoded into, with how many arrays and objects enclose it."""
    yield 0, manifest
    if not isinstance(manifest.metadata, msgspec.UnsetType):
        yield 1, manifest.metadata
    sections = (manifest.nodes, manifest.sources, manifest.exposures)
    nodes = [(2, node) for section in sections if isinstance(section, dict) for node in section.values()]
    if isinstance(manifest.disabled, dict):
        nodes.extend((3, node) for entries in manifest.disabled.values() for node in entries)
    for depth, node in nodes:
        yield depth, node
        if isinstance(node.config, _Config):
            yield depth + 1, node.config
        if isinstance(node.columns, dict):
            yield from ((depth + 2, column) for column in node.columns.values())


def _deeper(value: dict[Any, Any] | list[Any], levels: int) -> bool:
    """Whether `value`, an array or object, opens more than `levels` levels itself included.

    `json_files.nests_deeper_than` gives the same answer for any value; this one is for the many
    small values of a large manifest, where the common case is flat. It recurses at most `levels`
    deep, within `MAX_JSON_DEPTH`, so far inside Python's recursion limit.
    """
    if levels <= 0:
        return True
    # Exact types are what a JSON decode builds, and the check is cheaper than isinstance.
    for child in value.values() if type(value) is dict else value:
        if (type(child) is dict or type(child) is list) and _deeper(child, levels - 1):
            return True
    return False
