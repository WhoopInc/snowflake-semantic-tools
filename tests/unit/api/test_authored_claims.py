"""SST reports what the authored-file tables claim, in every reference-project file of each kind.

For each field a table declares, at the first reference file holding it, a session overlay
writes one edit and checks that validation reports something it did not before: the field
removed when it is required, a value outside its choices when it is an enum, and a value of
the wrong kind. `LENIENT` names the fields SST reads without checking their kind today, so a
value of the wrong kind there is read as text or truthiness; each must still be silent, so the
list only shrinks.
"""

from __future__ import annotations

import functools
from collections import Counter
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.roundtrip import load_editable
from snowflake_semantic_tools.api import ProjectSession
from snowflake_semantic_tools.domain.model.authored import NodePath
from snowflake_semantic_tools.domain.model.authored_schema import (
    FILE_KINDS,
    Field,
    FieldStatus,
    FileKind,
    Node,
    ValueKind,
)
from tests.helpers.reference_project import DBT_MANIFEST, FIXTURE, project_copy

# Where each kind's files live in the reference project. dbt `meta.sst` arrives through the
# manifest, which a file overlay does not change, so the dbt kinds are not probed here.
_FILES = {
    "semantic_view": "semantic_models/semantic_views/**/*.yml",
    "metric": "semantic_models/metrics/*.yml",
    "filter": "semantic_models/filters/*.yml",
    "custom_instruction": "semantic_models/custom_instructions/*.yml",
    "verified_query": "semantic_models/verified_queries/*.yml",
    "relationship": "semantic_models/relationships/*.yml",
    "tool": "tools/**/*.yml",
    "agent": "agents/**/agent.yml",
    "cowork_profile": "cowork_profiles/**/cowork_profile.yml",
    "plugin": "plugins/**/plugin.yml",
    "coco_profile": "coco_profiles/*/coco_profile.yml",
    "eval_metric": "eval_metrics/*.yml",
}
# A value of each kind's wrong type.
_WRONG: Mapping[ValueKind, object] = {
    ValueKind.STRING: {"x": 1},
    ValueKind.TEMPLATE: {"x": 1},
    ValueKind.ENUM: {"x": 1},
    ValueKind.BOOLEAN: "maybe",
    ValueKind.INTEGER: "abc",
    ValueKind.NUMBER: "abc",
    ValueKind.LIST: {"x": 1},
    ValueKind.MAPPING: [1],
}
# Fields whose value SST reads without checking its kind, by `<kind>:<field>`: text read with
# `str()`, so a mapping is published as its repr; flags read for truthiness; and agent and
# CoWork display and tool settings read as optional, so a wrong value reads as unset.
LENIENT: frozenset[str] = frozenset(
    (
        # Descriptions and question text, read with str().
        "custom_instruction:description",
        "custom_instruction:ai_question_categorization",
        "custom_instruction:ai_sql_generation",
        "filter:description",
        "metric:description",
        "relationship:description",
        "semantic_view:variables.description",
        "verified_query:description",
        "verified_query:question",
        "tool:description",
        "tool:columns.description",
        "tool:columns.type",
        "cowork_profile:comment",
        "cowork_profile:spec.subagents.description",
        # Flags read for truthiness, and lists read as empty when they are not one.
        "tool:columns.filterable",
        "tool:columns.searchable",
        "tool:on.attributes",
        # Display blocks and tool settings read as optional.
        "agent:profile",
        "agent:profile.avatar",
        "agent:profile.color",
        "agent:profile.display_name",
        "cowork_profile:display",
        "cowork_profile:display.color",
        "cowork_profile:display.display_name",
        "agent:spec.tools.max_results",
        "agent:spec.tools.query_timeout",
        "agent:spec.tools.warehouse",
    )
)


@dataclass(frozen=True, slots=True)
class Claim:
    """One edit a table says SST reports: where it is made, and what it writes."""

    kind: str
    field: str
    claim: str
    file: str
    path: NodePath
    key: str
    value: object

    @property
    def id(self) -> str:
        """Return the claim as the test names it: `<kind>:<field>:<claim>`."""
        return f"{self.kind}:{self.field}:{self.claim}"


def _declarations(tree: Mapping[str, Any], kind: FileKind) -> Iterator[tuple[NodePath, Mapping[str, Any]]]:
    if not kind.roots:
        yield (), tree
        return
    one, many = kind.roots
    if isinstance(tree.get(one), Mapping):
        yield (one,), tree[one]
    for index, item in enumerate(tree.get(many) or ()):
        if isinstance(item, Mapping):
            yield (many, index), item


def _present(node: Node, value: object, path: NodePath) -> Iterator[tuple[Node, Mapping[str, Any], NodePath]]:
    """Each declared node `value` holds, with its path, depth-first."""
    if not isinstance(value, Mapping):
        return
    yield node, value, path
    for item in node.all_fields():
        nested = item.nested if item.read else None
        if nested is None or item.name not in value:
            continue
        inner = value[item.name]
        if isinstance(item.items, Node):
            for index, entry in enumerate(inner if isinstance(inner, list) else ()):
                yield from _present(nested, entry, (*path, item.name, index))
        else:
            yield from _present(nested, inner, (*path, item.name))


def _current(node: Node, mapping: Mapping[str, Any]) -> list[Field]:
    """The current fields of `node` that apply to `mapping`: its own, then its variant's."""
    fields = list(node.fields)
    if node.discriminator is not None:
        fields += node.variants.get(str(mapping.get(node.discriminator)), ())
    return [item for item in fields if item.status is FieldStatus.CURRENT]


def _edits(item: Field) -> Iterator[tuple[str, object]]:
    if item.required:
        yield "required", None
    if item.kind is ValueKind.ENUM and not item.open_choices:
        yield "choice", "zz_not_a_choice"
    if item.kind in _WRONG:
        yield "kind", _WRONG[item.kind]


def _claims() -> list[Claim]:
    """The first place each claim can be made in the reference project, kind by kind, file by file."""
    found: dict[str, Claim] = {}
    for name, pattern in _FILES.items():
        kind = FILE_KINDS[name]
        for file in sorted(FIXTURE.glob(pattern)):
            relative = file.relative_to(FIXTURE).as_posix()
            tree = load_editable(file.read_text(encoding="utf-8"), relative).root
            if not isinstance(tree, Mapping):
                continue
            for start, declaration in _declarations(tree, kind):
                for node, mapping, path in _present(kind.node, declaration, start):
                    where = ".".join(str(part) for part in path[len(start) :] if isinstance(part, str))
                    for item in _current(node, mapping):
                        if item.name not in mapping:
                            continue
                        field = f"{where}.{item.name}" if where else item.name
                        for claim, value in _edits(item):
                            made = Claim(name, field, claim, relative, path, item.name, value)
                            found.setdefault(made.id, made)
    return sorted(found.values(), key=lambda made: made.id)


CLAIMS = _claims()


@pytest.fixture(scope="module")
def session(tmp_path_factory: pytest.TempPathFactory) -> ProjectSession:
    return ProjectSession.open(project_copy(tmp_path_factory.mktemp("claims")), manifest_path=DBT_MANIFEST)


def _edited(project: Path, claim: Claim) -> tuple[bytes, bytes]:
    """The claim's file written back unedited, then with the claim's edit, in the file's own layout."""
    text = (project / claim.file).read_text(encoding="utf-8")
    plain = load_editable(text, claim.file)
    edited = load_editable(text, claim.file)
    target: Any = edited.root
    for part in claim.path:
        target = target[part]
    if claim.claim == "required":
        del target[claim.key]
    else:
        target[claim.key] = claim.value
    return plain.dump().encode(), edited.dump().encode()


@functools.cache
def _codes_of(session: ProjectSession, file: str, data: bytes) -> Counter[str]:
    try:
        return Counter(str(item["code"]) for item in session.with_overlay({file: data}).validate().diagnostics)
    except ProjectError as error:
        # A file SST cannot read stops the run, which is reported too.
        return Counter(item.code for item in error.diagnostics) or Counter(("raised",))


@pytest.mark.parametrize("claim", CLAIMS, ids=lambda claim: claim.id)
def test_sst_reports_what_the_table_claims(session: ProjectSession, claim: Claim) -> None:
    plain, edited = _edited(session.project_dir, claim)
    reported = bool(_codes_of(session, claim.file, edited) - _codes_of(session, claim.file, plain))
    lenient = f"{claim.kind}:{claim.field}" in LENIENT and claim.claim == "kind"
    if lenient:
        assert not reported, f"{claim.id} is reported now: drop it from LENIENT"
    else:
        assert reported, f"{claim.file}: SST reports nothing for {claim.id}"


def test_every_lenient_field_is_one_a_table_declares() -> None:
    assert {f"{claim.kind}:{claim.field}" for claim in CLAIMS if claim.claim == "kind"} >= LENIENT
