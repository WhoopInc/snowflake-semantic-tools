"""SST reports what the authored-file tables claim, in every reference-project file of each kind.

For each field a table declares, at the first reference file holding it, a session overlay
writes one edit and checks that validation reports something it did not before: the field
removed when it is required, a value outside its choices when it is an enum, and a value of
the wrong kind. A field the table marks `loose` SST reads without checking its kind, so a value
of another kind there must be reported as SST-PRS131, not merely change something.
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
    walk,
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
    "eval_dataset": "agents/**/evals/*.yml",
    "eval_config": "agents/**/evals/*.yml",
}
# An evals folder holds both kinds: a dataset is the file that lists `questions:`.
_HOLDS = {"eval_dataset": True, "eval_config": False}
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
# A loose field holding another kind is reported with this code; any other claim with any code.
_LOOSE_CODE = "SST-PRS131"


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
    declared: Field

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
            if not isinstance(tree, Mapping) or _HOLDS.get(name, "questions" in tree) != ("questions" in tree):
                continue
            for start, declaration in _declarations(tree, kind):
                for node, mapping, path in _present(kind.node, declaration, start):
                    where = ".".join(str(part) for part in path[len(start) :] if isinstance(part, str))
                    for item in _current(node, mapping):
                        if item.name not in mapping:
                            continue
                        field = f"{where}.{item.name}" if where else item.name
                        for claim, value in _edits(item):
                            made = Claim(name, field, claim, relative, path, item.name, value, item)
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
    new = _codes_of(session, claim.file, edited) - _codes_of(session, claim.file, plain)
    if claim.declared.loose and claim.claim == "kind":
        # What SST then reads (an unset description, say) may draw its own report too.
        assert _LOOSE_CODE in new, f"{claim.file}: {claim.id} reports {sorted(new)}"
    else:
        assert new, f"{claim.file}: SST reports nothing for {claim.id}"


def test_every_loose_field_is_probed_somewhere() -> None:
    """A field one node shares with another, such as a display block's, is the same field wherever it sits."""
    loose = {id(item) for name in _FILES for _, _, item in walk(FILE_KINDS[name].node) if item.loose}
    probed = {id(claim.declared) for claim in CLAIMS if claim.declared.loose and claim.claim == "kind"}
    assert loose == probed
