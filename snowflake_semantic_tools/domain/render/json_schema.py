"""Render the authored-file tables and the configuration key table as JSON Schemas (Draft 2020-12).

One schema per file kind, from its table, and one for `sst_config.yml`, from `CONFIG_SCHEMA`, so
an editor checks a file as SST would. A schema flags only what SST reports: a key it does not
declare, a required key missing, a value outside an enum or of another kind. It accepts
everything SST accepts: a `config:` value that is a template, an enum SST matches ignoring
case, a key of an older dialect (marked `deprecated`, its description naming what replaced it).
Each `$id` is a name, never fetched, and only the keywords in `SCHEMA_KEYWORDS` are emitted.
Every key, file kind and the configuration file carry `x-studio`, an annotation no validator
reads: how an editor names, groups and lays it out, from `domain.model.presentation`.
"""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.domain.model.authored_schema import (
    FILE_KINDS,
    SEMANTIC_KINDS,
    Field,
    FieldStatus,
    FileKind,
    Node,
    ValueKind,
)
from snowflake_semantic_tools.domain.model.config_schema import CHILDREN, ConfigKey, KeyKind, KeyStatus, block_settings
from snowflake_semantic_tools.domain.model.presentation import (
    CONFIG_SECTIONS,
    CONFIG_UI,
    FIELD_UI,
    KIND_UI,
    LIST_UI,
    KindUi,
    ListUi,
    Ui,
)
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY
from snowflake_semantic_tools.domain.resolve.ref_fields import REFERENCE_FIELDS_AT

DRAFT = "https://json-schema.org/draft/2020-12/schema"
# The annotation an editor reads a key's presentation from; validators ignore it, as JSON Schema
# ignores any keyword it does not define.
STUDIO = "x-studio"
# The names the configuration file's schema, and a semantic models file's, go by.
CONFIG_KIND = "sst_config"
SEMANTIC_FILE = "semantic_file"
# Every keyword a schema may hold; a test refuses any other, and so does the test-only checker.
SCHEMA_KEYWORDS = frozenset(
    (
        "$schema",
        "$id",
        "$defs",
        "$ref",
        "title",
        "description",
        "type",
        "properties",
        "required",
        "additionalProperties",
        "patternProperties",
        "enum",
        "items",
        "anyOf",
        "allOf",
        "if",
        "then",
        "pattern",
        "minimum",
        "maximum",
        "deprecated",
        STUDIO,
    )
)
_TYPES: Mapping[ValueKind, str] = {
    ValueKind.STRING: "string",
    ValueKind.TEMPLATE: "string",
    ValueKind.BOOLEAN: "boolean",
    ValueKind.INTEGER: "integer",
    ValueKind.NUMBER: "number",
    ValueKind.LIST: "array",
    ValueKind.MAPPING: "object",
}
# A template, which a configuration value of any kind may be in place of its value.
_TEMPLATE_TEXT: dict[str, object] = {"type": "string", "pattern": r"\{\{|\{%"}


def schema_id(kind: str) -> str:
    """Return a schema's `$id`: a name that identifies it, never a location that is fetched."""
    return f"urn:snowflake-semantic-tools:schema:{kind}"


def any_case(choices: tuple[str, ...]) -> str:
    """Return a pattern matching any of `choices` in any case, as JSON Schema patterns take no flags."""
    spelled = ("".join(f"[{c.lower()}{c.upper()}]" if c.isalpha() else _escaped(c) for c in item) for item in choices)
    return "^(?:" + "|".join(spelled) + ")$"


def _escaped(character: str) -> str:
    return "\\" + character if character in r".^$*+?()[]{}|\/" else character


def file_schemas() -> dict[str, dict[str, object]]:
    """Return every file kind's schema, and the configuration file's, by kind, in reference order."""
    found = {name: file_schema(kind) for name, kind in FILE_KINDS.items()}
    found[SEMANTIC_FILE] = semantic_file_schema()
    found[CONFIG_KIND] = config_schema()
    return found


def semantic_file_schema() -> dict[str, object]:
    """Return the schema of one file in the semantic models directory, which may declare any semantic kind.

    A root key no registered type owns is reported (SST-PRS001); one a type other than the six
    semantic kinds owns is left to that type's own checks.
    """
    properties: dict[str, object] = {}
    roots = [(entry.singular_root, entry.root_key) for entry in ARTIFACT_REGISTRY.artifacts.values()]
    roots += [(entry.singular_root, entry.root_key) for entry in ARTIFACT_REGISTRY.members.values()]
    for pair in roots:
        for root in pair:
            if root is not None:
                properties[root] = {}
    for kind in SEMANTIC_KINDS:
        properties.update(_roots(kind))
    return {
        "$schema": DRAFT,
        "$id": schema_id(SEMANTIC_FILE),
        "title": "A file in the semantic models directory: views and the members they attach",
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }


def file_schema(kind: FileKind) -> dict[str, object]:
    """Return the schema of one file kind: its declaration, under its roots when it has them."""
    node = _node(kind.node, kind.name)
    if not kind.roots:
        body = node
    else:
        body = {"type": "object", "properties": _roots(kind)}
        if kind.name == "tool":
            # A tools file holds members alone, so another key is reported; a semantic file may
            # hold other kinds' roots as well.
            body["additionalProperties"] = False
    return {
        "$schema": DRAFT,
        "$id": schema_id(kind.name),
        "title": kind.summary,
        STUDIO: _kind_ui(KIND_UI[kind.name]),
        **body,
    }


def _roots(kind: FileKind) -> dict[str, object]:
    """The two keys a kind's declarations are written under: one declaration, and a list of them.

    Each declaration carries its kind's presentation, its sections among it, so an editor reaching
    one through its root key reads the same `x-studio` as the kind's own schema gives.
    """
    node = {**_node(kind.node, kind.name), STUDIO: _kind_ui(KIND_UI[kind.name])}
    one, many = kind.roots
    return {
        one: node,
        many: {"type": "array", "items": node, STUDIO: {"label": KIND_UI[kind.name].plural}},
    }


def _node(node: Node, path: str) -> dict[str, object]:
    """The schema of one node; `path` is its place as `REFERENCE_FIELDS` names fields: `agent.spec.tools`."""
    properties = {item.name: _field(item, f"{path}.{item.name}") for item in node.all_fields()}
    schema: dict[str, object] = {"type": "object", "properties": properties}
    required = [item.name for item in node.fields if item.required and item.read]
    if required:
        schema["required"] = required
    if not node.open:
        schema["additionalProperties"] = False
    if node.discriminator is not None:
        schema["allOf"] = _variants(node)
    if node.shorthand is not None:
        return {"anyOf": [{"type": _TYPES[node.shorthand]}, schema]}
    return schema


def _variants(node: Node) -> list[object]:
    """Per discriminator value, refuse the keys only other values take, as SST reports them."""
    every = {item.name for item in node.all_fields()}
    own = {item.name for item in node.fields}
    rules: list[object] = []
    for value, fields in node.variants.items():
        taken = own | {item.name for item in fields}
        refused = dict.fromkeys(sorted(every - taken), False)
        if refused:
            rules.append(
                {
                    "if": {
                        "properties": {str(node.discriminator): {"enum": [value]}},
                        "required": [node.discriminator],
                    },
                    "then": {"properties": refused},
                }
            )
    return rules


def _field(item: Field, path: str) -> dict[str, object]:
    """The schema of one field's value, with its description and, for an older key, `deprecated`."""
    schema = _value(item, path)
    description = item.summary
    calls = REFERENCE_FIELDS_AT.get(path)
    if calls:
        description += " Takes " + ", ".join(f"`{call}()`" for call in calls) + "."
    schema["description"] = description
    schema[STUDIO] = _ui(FIELD_UI[path], LIST_UI.get(path))
    if item.status is not FieldStatus.CURRENT:
        schema["deprecated"] = True
    return schema if item.required else _nullable(schema)


def _nullable(schema: dict[str, object]) -> dict[str, object]:
    """`schema`, also accepting null: SST reads an optional field written as null as unset."""
    if "type" in schema:
        types = schema["type"]
        schema["type"] = [*(types if isinstance(types, list) else [types]), "null"]
    elif "enum" in schema:
        schema["enum"] = [*schema["enum"], None]  # type: ignore[misc]
    elif "anyOf" in schema:
        schema["anyOf"] = [*schema["anyOf"], {"type": "null"}]  # type: ignore[misc]
    return schema


def _value(item: Field, path: str) -> dict[str, object]:
    """The schema of the value a field holds, by its kind; any value when SST does not read it."""
    if not item.read or item.kind is ValueKind.ANY:
        return {}
    if item.config_block is not None:
        return _settings(item.config_block, item.node, path)
    if item.kind is ValueKind.ENUM:
        return _choices(item)
    if item.kind is ValueKind.LIST:
        return {"type": "array", "items": _item(item.items, path)}
    if item.kind is ValueKind.MAPPING:
        return _node(item.node, path) if item.node is not None else {"type": "object"}
    types = [_TYPES[kind] for kind in (item.kind, *item.also) if kind in _TYPES]
    return {"type": types[0] if len(types) == 1 else types}


def _item(items: ValueKind | Node | None, path: str) -> dict[str, object]:
    if isinstance(items, Node):
        return _node(items, path)
    return {"type": _TYPES[items]} if items is not None and items in _TYPES else {}


def _choices(item: Field) -> dict[str, object]:
    if item.open_choices:
        return {"type": "string"}
    if item.folded:
        return {"type": "string", "pattern": any_case(item.choices)}
    return {"enum": list(item.choices)}


def _settings(block: str, node: Node | None, path: str) -> dict[str, object]:
    """A `config:` field: its block's settings, each typed as `sst_config.yml` types it."""
    properties: dict[str, object] = {
        name: {**_config_value(key), STUDIO: _ui(CONFIG_UI[key.path])} for name, key in block_settings(block).items()
    }
    for item in node.fields if node is not None else ():
        properties[item.name] = _field(item, f"{path}.{item.name}")
    return {"type": "object", "properties": properties, "additionalProperties": False}


def config_schema() -> dict[str, object]:
    """Return the schema of `sst_config.yml`, from `CONFIG_SCHEMA`."""
    definitions: dict[str, object] = {}
    body = _config_block("", definitions)
    schema: dict[str, object] = {
        "$schema": DRAFT,
        "$id": schema_id(CONFIG_KIND),
        "title": "The SST project file",
        STUDIO: {"sections": [{"id": s.id, "label": s.label, "blurb": s.blurb} for s in CONFIG_SECTIONS]},
    }
    schema.update(body)
    if definitions:
        schema["$defs"] = definitions
    return schema


def _config_block(parent: str, definitions: dict[str, object]) -> dict[str, object]:
    """The schema of the block at `parent`: its declared keys, then its name or route slot."""
    children = CHILDREN.get(parent, {})
    properties = {name: _config_key(key, definitions) for name, key in children.items() if not name.startswith("<")}
    schema: dict[str, object] = {"type": "object", "properties": properties}
    slot = children.get("<name>") or children.get("<route>")
    if slot is None:
        schema["additionalProperties"] = False
    elif slot.name == "<name>":
        schema["additionalProperties"] = _config_key(slot, definitions)
    else:
        # A route nests the block's settings and further routes, so it refers to itself.
        route = f"{parent}.route"
        if route not in definitions:
            definitions[route] = {}
            settings = {name: _config_key(key, definitions) for name, key in children.items() if name.startswith("+")}
            definitions[route] = {
                "type": "object",
                "description": slot.summary,
                STUDIO: _ui(CONFIG_UI[slot.path]),
                "properties": settings,
                "patternProperties": {"^[^+]": {"$ref": f"#/$defs/{route}"}},
                "additionalProperties": False,
            }
        schema["patternProperties"] = {"^[^+]": {"$ref": f"#/$defs/{route}"}}
        schema["additionalProperties"] = False
    return schema


def _config_key(key: ConfigKey, definitions: dict[str, object]) -> dict[str, object]:
    schema = _config_block(key.path, definitions) if key.kind is KeyKind.BLOCK else _config_value(key)
    schema["description"] = key.summary
    schema[STUDIO] = _ui(CONFIG_UI[key.path])
    if key.status is not KeyStatus.CURRENT:
        schema["deprecated"] = True
    return schema


def _config_value(key: ConfigKey) -> dict[str, object]:
    """A setting's value as SST reads it: text, or a value of its kind, or a template standing for one."""
    if key.status is not KeyStatus.CURRENT or key.kind is KeyKind.ANY:
        return {"description": key.summary, "deprecated": True} if key.status is not KeyStatus.CURRENT else {}
    if key.kind is KeyKind.BLOCK:
        return {"type": "object"}
    if key.kind is KeyKind.MAP:
        return {"type": "object", "description": key.summary}
    if key.kind is KeyKind.ENUM:
        chosen: dict[str, object] = (
            {"type": "string", "pattern": any_case(key.choices)} if key.folded else {"enum": list(key.choices)}
        )
        return {"anyOf": [chosen, _TEMPLATE_TEXT], "description": key.summary}
    typed = {
        KeyKind.BOOLEAN: "boolean",
        KeyKind.INTEGER: "integer",
        KeyKind.LIST: "array",
    }.get(key.kind)
    if typed is None:
        return {"type": "string", "description": key.summary}
    value: dict[str, object] = {"type": typed}
    if key.minimum is not None:
        value["minimum"] = key.minimum
    if key.maximum is not None:
        value["maximum"] = key.maximum
    return {"anyOf": [value, _TEMPLATE_TEXT], "description": key.summary}


def _ui(ui: Ui, listed: ListUi | None = None) -> dict[str, object]:
    """Return a key's `x-studio`: its label, what else it sets, and a list's item naming and layout."""
    found: dict[str, object] = {"label": ui.label}
    if ui.group is not None:
        found["group"] = ui.group
    if ui.advanced:
        found["advanced"] = True
    if ui.widget is not None:
        found["widget"] = ui.widget.value
    if listed is not None:
        item: dict[str, object] = {"noun": listed.noun, "title": listed.title, "layout": listed.layout.value}
        if listed.subtitle is not None:
            item["subtitle"] = listed.subtitle
        if listed.columns:
            item["columns"] = list(listed.columns)
        found["list"] = item
    return found


def _kind_ui(ui: KindUi) -> dict[str, object]:
    """A file kind's `x-studio`: its names, its sections in order, and the keys that describe it."""
    return {
        "label": ui.label,
        "plural": ui.plural,
        "groups": [{"id": group.id, "label": group.label, "blurb": group.blurb} for group in ui.groups],
        "description": ui.description,
        "owner": ui.owner,
        "key_settings": list(ui.key_settings),
        "flatten": list(ui.flatten),
    }
