"""Report every authored key the loader does not read, and nodes with a missing or repeated name."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.adapters.yaml.documents import NodePath, RawDocument, RawDocuments
from snowflake_semantic_tools.adapters.yaml.semantic.nodes import _node_origin, _node_root
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.reference import TemplateSyntaxError, scan_template_calls

# The keys the loader reads, per semantic-model node. Anything else is reported,
# because a key the loader skips changes nothing in the DDL and would otherwise
# pass review looking as though it does.
AUTHORED_KEYS: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "semantic_view": frozenset(
            (
                "name",
                "description",
                "tables",
                "table_config",
                "custom_instructions",
                "variables",
                "tags",
                "max_staleness",
                "enabled",
            )
        ),
        "metric": frozenset(
            (
                "name",
                "expr",
                "description",
                "synonyms",
                "tables",
                "derived",
                "using_relationships",
                "non_additive_dimensions",
                "access_modifier",
                "window",
            )
        ),
        "filter": frozenset(("name", "expr", "description", "tables", "labels")),
        # Snowflake has no clause for a description on the next three: it documents
        # the YAML for its readers and is never published.
        "custom_instruction": frozenset(("name", "description", "ai_sql_generation", "ai_question_categorization")),
        "verified_query": frozenset(
            (
                "name",
                "description",
                "question",
                "sql",
                "sql_file",
                "tables",
                "verified_at",
                "verified_by",
                "use_as_onboarding_question",
            )
        ),
        "relationship": frozenset(("name", "description", "left_table", "right_table", "relationship_conditions")),
    }
)
# Mappings below a node, by the scope that holds them: a block, each item of a list
# field, or each value of a name map.
NESTED_KEYS: Mapping[tuple[str, str], frozenset[str]] = MappingProxyType(
    {
        ("semantic_view", "table_config"): frozenset(("synonyms", "distinct_range")),
        ("semantic_view", "variables"): frozenset(("name", "data_type", "default_value", "description")),
        ("semantic_view", "tags"): frozenset(("name", "value")),
        ("metric", "non_additive_dimensions"): frozenset(("dimension", "table", "sort_direction", "null_order")),
        ("metric", "window"): frozenset(("partition_by", "partition_by_excluding", "order_by", "frame")),
        ("metric.window", "order_by"): frozenset(("ref", "sort_direction", "null_order")),
    }
)
NAME_MAPS = frozenset((("semantic_view", "table_config"),))
# Fields whose value is one mapping; every other nested field is a list.
BLOCKS = frozenset((("metric", "window"),))
# 0.3 spellings, by the node or entry that carries them. Each is an error naming
# the 1.0 key: reading the old spelling would keep two dialects alive.
RENAMED_KEYS: Mapping[tuple[str, str], str] = MappingProxyType(
    {
        ("custom_instruction", "sql_generation"): "ai_sql_generation",
        ("custom_instruction", "question_categorization"): "ai_question_categorization",
        ("relationship", "relationship_columns"): "relationship_conditions",
        ("metric", "visibility"): "access_modifier",
        ("metric", "non_additive_by"): "non_additive_dimensions",
        ("metric.non_additive_dimensions", "order"): "sort_direction",
        ("metric.non_additive_dimensions", "nulls"): "null_order",
        ("metric.window.order_by", "column"): "ref",
        ("metric.window.order_by", "direction"): "sort_direction",
    }
)


def _authored_key_diagnostics(documents: RawDocuments) -> tuple[Diagnostic, ...]:
    """Every key the loader does not read: SST-PRS020 for a 0.3 spelling, SST-PRS004 otherwise."""
    diagnostics: list[Diagnostic] = []
    for document in documents.documents:
        for node_type, allowed in AUTHORED_KEYS.items():
            root_key = _node_root(node_type)
            nodes = document.tree.get(root_key) if root_key in document.root_keys else None
            for index, node in enumerate(nodes if isinstance(nodes, list) else ()):
                if isinstance(node, dict):
                    subject = artifact_key(node_type, str(node.get("name") or index))
                    diagnostics.extend(_unread_keys(document, subject, node_type, (root_key, index), "", node, allowed))
    return tuple(diagnostics)


# Types whose duplicates SST-VAL001 already reports.
_NAMED_ELSEWHERE = frozenset(("semantic_view", "metric"))


def _member_name_diagnostics(documents: RawDocuments) -> tuple[Diagnostic, ...]:
    """A node with no name (SST-PRS107) or a name its type already uses (SST-PRS106).

    Either would otherwise be skipped or overwritten without a word.
    """
    diagnostics: list[Diagnostic] = []
    seen: set[tuple[str, str]] = set()
    for document in documents.documents:
        for node_type in AUTHORED_KEYS:
            root_key = _node_root(node_type)
            nodes = document.tree.get(root_key) if root_key in document.root_keys else None
            for index, node in enumerate(nodes if isinstance(nodes, list) else ()):
                origin = _node_origin(document, root_key, index)
                if not isinstance(node, dict) or not node.get("name"):
                    diagnostics.append(
                        D(
                            "SST-PRS107",
                            artifact=document.path,
                            member_type=node_type,
                            index=index,
                            subject=artifact_key(node_type, str(index)),
                            origin=origin,
                        )
                    )
                    continue
                name = str(node["name"])
                if node_type in _NAMED_ELSEWHERE:
                    continue
                if (node_type, name.casefold()) in seen:
                    diagnostics.append(
                        D(
                            "SST-PRS106",
                            artifact=document.path,
                            member_type=node_type,
                            name=name,
                            subject=artifact_key(node_type, name),
                            origin=origin,
                        )
                    )
                seen.add((node_type, name.casefold()))
    return tuple(diagnostics)


def _unread_keys(
    document: RawDocument,
    subject: str,
    scope: str,
    path: NodePath,
    prefix: str,
    mapping: Mapping[Any, Any],
    allowed: frozenset[str],
) -> list[Diagnostic]:
    """Report each key of `mapping`, and of the mappings nested in it, that the loader does not read.

    `scope` is the node type, or `<type>.<field>` for a nested mapping, and picks both the nested
    fields to descend into and the 0.3 spellings to name. A key is labelled `prefix` plus its
    name, such as `window.order_by[0].column`, and located by its node path. The mapping's own
    keys come first, in authored order, then each nested field's, in `NESTED_KEYS` order; a
    nested field of the wrong shape is left to its own check.

    Diagnostics:
        SST-PRS020: when a key is the 0.3 spelling of a 1.0 key in its scope.
        SST-PRS004: when any other key is not one `allowed` lists.
    """
    diagnostics = [
        _unread_key(document, (*path, str(key)), scope, f"{prefix}{key}", subject)
        for key in mapping
        if str(key) not in allowed
    ]
    for (owner, field), keys in NESTED_KEYS.items():
        value = mapping.get(field) if owner == scope else None
        if isinstance(value, list):
            entries: tuple[tuple[str | int | None, object, str], ...] = tuple(
                (key, entry, f"{prefix}{field}[{key}].") for key, entry in enumerate(value)
            )
        elif isinstance(value, dict) and (owner, field) in NAME_MAPS:
            entries = tuple((str(key), entry, f"{prefix}{field}.{key}.") for key, entry in value.items())
        elif isinstance(value, dict) and (owner, field) in BLOCKS:
            entries = ((None, value, f"{prefix}{field}."),)
        else:
            # Absent, or the wrong shape, which the field's own check reports.
            entries = ()
        for key, entry, label in entries:
            if isinstance(entry, dict):
                where: NodePath = (*path, field) if key is None else (*path, field, key)
                diagnostics.extend(_unread_keys(document, subject, f"{owner}.{field}", where, label, entry, keys))
    return diagnostics


def _unread_key(document: RawDocument, path: NodePath, scope: str, field: str, subject: str) -> Diagnostic:
    position = document.position(path)
    origin = Origin(
        document.path,
        position.line if position is not None else None,
        position.col if position is not None else None,
    )
    renamed = RENAMED_KEYS.get((scope, str(path[-1])))
    if renamed is not None:
        return D("SST-PRS020", origin=origin, subject=subject, artifact=subject, field=field, expected=renamed)
    return D("SST-PRS004", origin=origin, subject=subject, artifact=subject, field=field)


def _legacy_reference_diagnostics(documents: RawDocuments) -> tuple[Diagnostic, ...]:
    """Report every legacy `table()` and `column()` global in every string of every document.

    Documents are read in order and each tree depth-first, so the calls come out in the order
    they are written.

    Diagnostics:
        SST-REF034: when a string calls `table()` with one argument.
        SST-REF035: when a string calls `column()` with two arguments.
    """
    return tuple(
        diagnostic for document in documents.documents for diagnostic in _legacy_calls(document.tree, document.path)
    )


def _legacy_calls(value: Any, file: str) -> Iterator[Diagnostic]:
    """Walk one YAML value depth-first: a string's own calls, then list items and mapping values."""
    if isinstance(value, str):
        yield from _legacy_string_calls(value, file)
    elif isinstance(value, list):
        for item in value:
            yield from _legacy_calls(item, file)
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from _legacy_calls(item, file)


def _legacy_string_calls(text: str, file: str) -> Iterator[Diagnostic]:
    """Report the legacy globals one string calls.

    A malformed template reports nothing here: the check that reads its field reports it.

    Diagnostics:
        SST-REF034: when the string calls `table()` with one argument.
        SST-REF035: when the string calls `column()` with two arguments.
    """
    try:
        calls = scan_template_calls(text)
    except TemplateSyntaxError:
        return
    for call in calls:
        if call.function == "table" and len(call.args) == 1:
            yield D(
                "SST-REF034",
                file=file,
                line=call.line,
                col=call.col,
                model=call.args[0],
            )
        elif call.function == "column" and len(call.args) == 2:
            yield D(
                "SST-REF035",
                file=file,
                line=call.line,
                col=call.col,
                model=call.args[0],
                column=call.args[1],
            )
