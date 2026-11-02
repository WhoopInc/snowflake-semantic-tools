"""What an editor may offer in each authored field: the template calls it takes, and its values.

SST knows, for every field of an authored file and every setting of `sst_config.yml`, which
template functions it takes (`REFERENCE_FIELDS`, `INLINE_FIELDS`) and which values it accepts:
the field's own `choices`, and the lists the project's `snowflake:` block configures. This puts
them together under the path the JSON Schemas describe each field by, so an editor completes and
limits a field as SST will read it. Nothing here reads a file; the configuration tree is handed in.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.agent import KNOWN_AGENT_TOOL_TYPES
from snowflake_semantic_tools.domain.model.authored_schema import FILE_KINDS, Field, Node, ValueKind
from snowflake_semantic_tools.domain.model.config_schema import (
    CONFIG_SCHEMA,
    ConfigKey,
    KeyKind,
    KeyStatus,
    avatar_allowlist,
    block_settings,
    config_block,
    extra_tool_types,
    orchestration_models,
)
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.resolve.calls import KNOWN_FUNCTIONS, LEGACY_FUNCTIONS, accepted_arity
from snowflake_semantic_tools.domain.resolve.ref_fields import INLINE_FIELDS, REFERENCE_FIELDS_AT
from snowflake_semantic_tools.domain.sql.datatype import SUGGESTED_TYPES

# What a non-artifact function's first argument names, for an editor to look its names up by.
_NON_ARTIFACT_NAMES: Mapping[str, str] = MappingProxyType(
    {
        "ref": "model",
        "source": "source",
        "var": "var",
        "tag": "tag",
        "file": "file",
        "extension": "extension",
        "eval_metric": "eval_metric",
        "eval_dataset": "eval_dataset",
        "eval_config": "eval_config",
    }
)
# The fields whose value is a Snowflake data type: the grammar decides, so the types are offered,
# not imposed.
DATA_TYPE_FIELDS: frozenset[str] = frozenset(
    ("semantic_view.variables.data_type", "tool.signature.type", "tool.returns", "dbt_column_meta.data_type")
)
# The template functions a setting's value may hold, as `sst_config.yml` resolves it: `var()`;
# `{{ target.<key> }}` names one of `TARGET_KEYS`.
SETTING_FUNCTIONS: tuple[str, ...] = ("var",)
TARGET_KEYS: tuple[str, ...] = ("database", "schema", "warehouse")


@dataclass(frozen=True, slots=True)
class TemplateFunction:
    """One template function an authored field may call.

    Attributes:
        name: The function, as `{{ <name>(...) }}` calls it.
        arity: The argument counts it accepts, fewest first.
        names: What its first argument names: a registry artifact type or member kind, such as
            `semantic_view` or `metric`, else `model`, `source`, `var`, `tag`, `file`,
            `extension`, `eval_metric`, `eval_dataset` or `eval_config`.
    """

    name: str
    arity: tuple[int, ...]
    names: str


@dataclass(frozen=True, slots=True)
class FieldVocabulary:
    """What one field takes, under the path the JSON Schemas describe it by.

    Attributes:
        path: The field's place: the file kind, then each key, list indices left out, such as
            `agent.spec.tools.semantic_view`; a setting of `sst_config.yml` is its key's path,
            such as `agents.+orchestration_model`, whichever route it is written under.
        functions: The template functions it takes, the one a bare value is told to use first;
            empty when it takes none.
        inline: Its calls sit amid other text, as in SQL or prose; False when a call is the
            whole value, or each item of a list.
        choices: The values it takes, or each item of a list takes; empty when any value goes.
        closed: A value outside `choices` is reported; False when they are suggestions.
        folded: `choices` are matched ignoring case.
    """

    path: str
    functions: tuple[str, ...] = ()
    inline: bool = False
    choices: tuple[str, ...] = ()
    closed: bool = False
    folded: bool = False


@dataclass(frozen=True, slots=True)
class FileRoot:
    """The top-level keys a file declares one kind under, such as a semantic file's `snowflake_metrics:`.

    Attributes:
        kind: The file kind, which a `FieldVocabulary.path` under these keys starts with.
        keys: The key one declaration is written under, then the key a list of them is.
    """

    kind: str
    keys: tuple[str, ...]


def file_roots() -> tuple[FileRoot, ...]:
    """Return the keys each kind declared under a top-level key is written under, in reference order."""
    return tuple(FileRoot(name, kind.roots) for name, kind in FILE_KINDS.items() if kind.roots)


def template_functions() -> tuple[TemplateFunction, ...]:
    """Return every template function a field may call, by name, but the legacy `table` and `column`."""
    names = {entry.ref_function: entry.name for entry in SEMANTIC_REGISTRY.artifacts.values() if entry.ref_function}
    names.update({entry.ref_function: entry.name for entry in SEMANTIC_REGISTRY.members.values() if entry.ref_function})
    return tuple(
        TemplateFunction(name, accepted_arity(name), names.get(name) or _NON_ARTIFACT_NAMES[name])
        for name in sorted(KNOWN_FUNCTIONS - LEGACY_FUNCTIONS)
    )


def field_vocabularies(config: Mapping[str, object]) -> tuple[FieldVocabulary, ...]:
    """Return what each field that takes calls or values takes, in file-kind order, then the settings.

    `config` is the project's `sst_config.yml` as resolved for a target; its `snowflake:` block
    closes the tool types, orchestration models and avatars a field may name.
    """
    project = _ProjectChoices(config_block(config.get("snowflake")))
    found: list[FieldVocabulary] = []
    for name, kind in FILE_KINDS.items():
        found.extend(_node(kind.node, name, project))
    for key in CONFIG_SCHEMA:
        vocabulary = _setting(key, key.path, project)
        if vocabulary is not None:
            found.append(vocabulary)
    return tuple(found)


@dataclass(frozen=True, slots=True)
class _ProjectChoices:
    """The values the project's `snowflake:` block allows, by the field or setting each closes."""

    snowflake: Mapping[str, object]

    def field(self, path: str) -> tuple[str, ...] | None:
        """The values a field may hold in this project; None when the project decides none."""
        choices = _FIELD_CHOICES.get(path)
        return None if choices is None else choices(self)

    def setting(self, name: str) -> tuple[str, ...] | None:
        """The values a setting may hold in this project, by its name without `+`; None when it decides none."""
        if name == "orchestration_model":
            return self.models()
        if name == "candidate_models" and isinstance(self.snowflake.get("orchestration_models"), list):
            # Each from the list when it is set (SST-VAL771), and never `auto` (SST-VAL770).
            return tuple(model for model in self.models() if model != "auto")
        return None

    def models(self) -> tuple[str, ...]:
        """`snowflake.orchestration_models`, sorted."""
        return tuple(sorted(orchestration_models(self.snowflake)))

    def tool_types(self) -> tuple[str, ...]:
        """The tool types SST renders and those `snowflake.tool_types` adds, sorted."""
        return tuple(sorted(KNOWN_AGENT_TOOL_TYPES | extra_tool_types(self.snowflake)))

    def avatars(self) -> tuple[str, ...] | None:
        """`snowflake.profile.avatar_allowlist`, sorted; None when it is unset, which allows any."""
        allowed = avatar_allowlist(self.snowflake)
        return None if allowed is None else tuple(sorted(allowed))


# The fields whose values the project's `snowflake:` block decides, with how it decides them.
_FIELD_CHOICES: Mapping[str, Callable[[_ProjectChoices], tuple[str, ...] | None]] = MappingProxyType(
    {
        "agent.spec.tools.type": _ProjectChoices.tool_types,
        "agent.profile.avatar": _ProjectChoices.avatars,
        "cowork_profile.display.avatar": _ProjectChoices.avatars,
        # A Cortex Code profile's model is checked against the same list (SST-VAL894).
        "coco_profile.model": _ProjectChoices.models,
    }
)


def _node(node: Node, path: str, project: _ProjectChoices) -> Iterator[FieldVocabulary]:
    for item in node.all_fields():
        if not item.read:
            continue
        place = f"{path}.{item.name}"
        if item.config_block is not None:
            for name, key in block_settings(item.config_block).items():
                vocabulary = _setting(key, f"{place}.{name}", project)
                if vocabulary is not None:
                    yield vocabulary
        vocabulary = _field(item, place, project)
        if vocabulary is not None:
            yield vocabulary
        nested = item.nested
        if nested is not None:
            yield from _node(nested, place, project)


def _field(item: Field, path: str, project: _ProjectChoices) -> FieldVocabulary | None:
    inline = INLINE_FIELDS.get(path)
    functions = (*REFERENCE_FIELDS_AT.get(path, ()), *sorted(inline.allowed if inline is not None else ()))
    choices: tuple[str, ...] = ()
    closed = False
    allowed = project.field(path)
    if allowed is not None:
        choices, closed = allowed, True
    elif item.kind is ValueKind.ENUM:
        choices, closed = item.choices, not item.open_choices
    elif path in DATA_TYPE_FIELDS:
        choices = SUGGESTED_TYPES
    if not functions and not choices:
        return None
    return FieldVocabulary(
        path,
        tuple(dict.fromkeys(functions)),
        inline=inline is not None,
        choices=choices,
        closed=closed,
        folded=item.folded and allowed is None,
    )


def _setting(key: ConfigKey, path: str, project: _ProjectChoices) -> FieldVocabulary | None:
    if key.status is not KeyStatus.CURRENT:
        return None
    allowed = project.setting(key.name.removeprefix("+"))
    if allowed is not None:
        return FieldVocabulary(path, choices=allowed, closed=True)
    if key.kind is KeyKind.ENUM:
        return FieldVocabulary(path, choices=key.choices, closed=True, folded=key.folded)
    return None
