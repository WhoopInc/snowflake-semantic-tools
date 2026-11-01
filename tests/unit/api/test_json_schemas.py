"""The generated JSON Schemas agree with SST: they flag what SST reports and accept what it accepts.

Every claim the authored-file tables make is probed in `test_authored_claims`, which edits a
reference file so SST reports it. Here the same edit must fail the schema `schema_for` names for
that file, at the edited key, and every unedited file must pass its schema. The schemas use only
the keywords in `SCHEMA_KEYWORDS`, which the test-only checker implements.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pytest
import yaml

from snowflake_semantic_tools.api import ProjectSession, schemas
from snowflake_semantic_tools.domain.model.authored_schema import FILE_KINDS
from snowflake_semantic_tools.domain.model.authored_schema.locations import schema_kind
from snowflake_semantic_tools.domain.render.json_schema import SCHEMA_KEYWORDS, any_case, schema_id
from tests.helpers.authored_claims import CLAIMS, Claim, edited
from tests.helpers.json_schema_check import check
from tests.helpers.reference_project import DBT_MANIFEST, project_copy
from tests.helpers.reference_project import FIXTURE as REFERENCE_PROJECT
from tests.helpers.seam_projects import parsed

# The kinds held in dbt's own files, whose `meta:` blocks no file-level schema checks.
_DBT_KINDS = frozenset(("dbt_model_meta", "dbt_column_meta"))
_SCHEMAS = schemas()
# The kinds the reference project writes a file of, each of which its files are checked against.
_REFERENCE_KINDS = frozenset(
    (
        "sst_config",
        "semantic_file",
        "tool",
        "agent",
        "cowork_profile",
        "skill",
        "plugin",
        "coco_profile",
        "coco_shared_profile",
        "eval_metric",
        "eval_dataset",
        "eval_config",
    )
)


@pytest.fixture(scope="module")
def session(tmp_path_factory: pytest.TempPathFactory) -> ProjectSession:
    return ProjectSession.open(project_copy(tmp_path_factory.mktemp("schemas")), manifest_path=DBT_MANIFEST)


def _keywords(schema: object) -> Iterator[str]:
    if isinstance(schema, dict):
        for key, value in schema.items():
            yield key
            nested = value.values() if key in ("properties", "patternProperties", "$defs") else (value,)
            for item in nested:
                yield from _keywords(item)
    elif isinstance(schema, list):
        for item in schema:
            yield from _keywords(item)


def test_every_schema_uses_only_the_declared_keywords() -> None:
    used = {keyword for schema in _SCHEMAS.values() for keyword in _keywords(schema)}
    assert used <= SCHEMA_KEYWORDS
    assert {schema["$id"] for schema in _SCHEMAS.values()} == {schema_id(kind) for kind in _SCHEMAS}


def test_every_schema_is_json() -> None:
    for kind, schema in _SCHEMAS.items():
        assert json.loads(json.dumps(schema)) == schema, kind


def test_every_file_kind_has_a_schema() -> None:
    assert set(FILE_KINDS) | {"semantic_file", "sst_config"} == set(_SCHEMAS)


def _authored_files() -> Iterator[str]:
    for path in sorted(REFERENCE_PROJECT.rglob("*")):
        relative = path.relative_to(REFERENCE_PROJECT).as_posix()
        if path.is_file() and not relative.startswith(("target/", "sst_target/", "dbt_packages/")):
            yield relative


def _tree(path: str, text: bytes | str) -> Any:
    """The file as SST reads it: YAML 1.2, so a key such as `on` stays text; a skill's front matter."""
    decoded = text.decode() if isinstance(text, bytes) else text
    if path.endswith("SKILL.md"):
        return yaml.safe_load(decoded.split("---\n")[1])
    return parsed(decoded, path).tree


def test_the_reference_project_passes_every_schema(session: ProjectSession) -> None:
    checked: dict[str, int] = {}
    for relative in _authored_files():
        kind = session.schema_for(relative)
        if kind is None:
            continue
        failures = check(_SCHEMAS[kind], _tree(relative, (REFERENCE_PROJECT / relative).read_bytes()))
        assert failures == [], (relative, kind, failures)
        checked[kind] = checked.get(kind, 0) + 1
    assert set(checked) == _REFERENCE_KINDS


def _target(claim: Claim) -> tuple[str | int, ...]:
    # A required claim is reported on the declaration; the others on the edited key.
    return claim.path if claim.claim == "required" else (*claim.path, claim.key)


_PROBED = [claim for claim in CLAIMS if claim.kind not in _DBT_KINDS]


@pytest.mark.parametrize("claim", _PROBED, ids=[claim.id for claim in _PROBED])
def test_the_schema_flags_what_sst_reports(session: ProjectSession, claim: Claim) -> None:
    kind = session.schema_for(claim.file)
    assert kind is not None, claim.file
    plain, changed = edited(session.project_dir, claim)
    assert check(_SCHEMAS[kind], _tree(claim.file, plain)) == []
    failures = check(_SCHEMAS[kind], _tree(claim.file, changed))
    assert any(path[: len(_target(claim))] == _target(claim) for path, _ in failures), failures


@pytest.mark.parametrize("kind", sorted(set(FILE_KINDS) - _DBT_KINDS))
def test_an_unknown_key_fails_where_it_is_written(kind: str) -> None:
    declaration = FILE_KINDS[kind]
    tree: dict[str, Any] = {item.name: _minimal(item.kind.value) for item in declaration.node.fields if item.required}
    holder = {declaration.roots[0]: tree} if declaration.roots else tree
    schema = _SCHEMAS["semantic_file" if declaration.roots and kind != "tool" else kind]
    if declaration.node.open:
        return
    tree["zz_unknown"] = 1
    where = (declaration.roots[0], "zz_unknown") if declaration.roots else ("zz_unknown",)
    assert (where, "false") in check(schema, holder)


def _minimal(kind: str) -> object:
    return {"list": [], "mapping": {}, "boolean": True, "integer": 1, "number": 1}.get(kind, "x")


def test_the_schema_declares_exactly_the_tables_keys() -> None:
    for name, kind in FILE_KINDS.items():
        if name in _DBT_KINDS:
            continue
        schema: Any = _SCHEMAS[name]
        body = schema["properties"][kind.roots[0]] if kind.roots else schema
        assert set(body["properties"]) == {item.name for item in kind.node.all_fields()}, name


@pytest.mark.parametrize(
    ("path", "kind"),
    [
        ("sst_config.yml", "sst_config"),
        ("semantic_models/views/sales.yml", "semantic_file"),
        ("tools/finance/fee.yaml", "tool"),
        ("agents/sales/agent.yml", "agent"),
        ("agents/sales/evals/smoke.yml", "eval_config"),
        ("cowork_profiles/desk/cowork_profile.yml", "cowork_profile"),
        ("cowork_profiles/desk/evals/routing.yml", "eval_config"),
        ("eval_metrics/tone.yml", "eval_metric"),
        ("skills/finance/close/SKILL.md", "skill"),
        ("plugins/finance/plugin.yml", "plugin"),
        ("coco_profiles/analyst/coco_profile.yml", "coco_profile"),
        ("coco_profiles/shared/coco_profile.yml", "coco_shared_profile"),
        ("models/orders.yml", None),
        ("agents/sales/notes.yml", None),
        ("skills/finance/close/reference/steps.md", None),
    ],
)
def test_schema_kind_follows_discovery(path: str, kind: str | None) -> None:
    assert schema_kind(path, {}) == kind


def test_schema_kind_follows_configured_directories() -> None:
    assert schema_kind("ai/agents/sales/agent.yml", {"agents_dir": "ai/agents"}) == "agent"
    assert schema_kind("agents/sales/agent.yml", {"agents_dir": "ai/agents"}) is None
    assert schema_kind("agents/sales/evals/qs.yml", {}, lists_questions=True) == "eval_dataset"


def test_a_folded_enum_is_matched_in_any_case() -> None:
    pattern = any_case(("sql", "python"))
    schema = {"type": "string", "pattern": pattern}
    assert [check(schema, value) for value in ("SQL", "Python", "sql")] == [[], [], []]
    assert check(schema, "ruby") == [((), "pattern")]


def test_a_config_value_may_be_a_template() -> None:
    config = _SCHEMAS["sst_config"]
    assert check(config, {"semantic_views": {"+enabled": "{{ target.name == 'prod' }}"}}) == []
    assert (("semantic_views", "+enabled"), "anyOf") in check(config, {"semantic_views": {"+enabled": "yes"}})
    assert check(config, {"semantic_views": {"finance": {"core": {"+enabled": False}}}}) == []
    assert check(config, {"semantic_viewz": {}}) == [(("semantic_viewz",), "false")]
