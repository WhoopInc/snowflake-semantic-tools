"""Every reference field is one template call, read by one helper; stage paths and tags resolve."""

from __future__ import annotations

from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.project import ParsedView
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.parse.template import TemplateCall
from snowflake_semantic_tools.domain.resolve.calls import required_call
from snowflake_semantic_tools.domain.resolve.membership import ViewMembers
from snowflake_semantic_tools.domain.resolve.ref_fields import REFERENCE_FIELDS, reference_functions
from snowflake_semantic_tools.domain.resolve.stage_path import resolve_stage_path, stage_member
from snowflake_semantic_tools.domain.resolve.tags import DeclaredTags, declared_tags, tag_reference
from snowflake_semantic_tools.domain.validate.semantic.scope import scope_diagnostics
from tests.helpers.semantic_members import MODELS

ORIGIN = Origin("agents/a/agent.yml", 4)
TARGET = TargetIdentity("dev", "acct", Identifier.parse("DEV_DB"), Identifier.parse("SALES"))


def _code(found: object) -> str | None:
    return found.code if isinstance(found, Diagnostic) else None


@pytest.mark.parametrize(
    ("value", "code"),
    [
        ("{{ relationship('a') }}", None),
        ("  {{ relationship('a') }}  ", None),
        ("orders_to_customers", "SST-REF050"),
        ("{{ relationship('a') }} and more", "SST-REF003"),
        ("{{ relationship('a') }}{{ relationship('b') }}", "SST-REF003"),
        ("{{ metric('a') }}", "SST-REF041"),
        ("{{ relationship('a', 'b') }}", "SST-REF015"),
        ("{{ relationship('a'", "SST-LOD004"),
    ],
)
def test_a_reference_field_is_exactly_one_call_of_its_function(value: str, code: str | None) -> None:
    found = required_call(value, "relationship", ORIGIN, field="using_relationships[0]", artifact="metric:m")
    assert _code(found) == code
    if code is None:
        assert isinstance(found, TemplateCall) and found.args == ("a",)


def test_a_bare_value_is_told_to_use_the_fields_first_function() -> None:
    found = required_call("sales", ("skill", "plugin", "extension"), None, field="source.path", artifact="agent:a")
    assert isinstance(found, Diagnostic) and found.code == "SST-REF050"
    assert found.message == "agent:a: source.path is 'sales'; it takes a {{ skill(...) }} call"
    # Any of the field's functions is accepted.
    assert isinstance(
        required_call("{{ plugin('kit') }}", ("skill", "plugin"), None, field="f", artifact="a"), TemplateCall
    )


def test_every_reference_field_names_known_functions_and_suggests_one_first() -> None:
    assert reference_functions("coco_profile.skills") == ("skill",)
    assert reference_functions("coco_profile.plugins") == ("plugin",)
    assert all(functions for functions in REFERENCE_FIELDS.values())
    with pytest.raises(KeyError):
        reference_functions("agent.name")


def _member(name: str, kind: str, *, relations: dict[str, str] | None = None) -> ToolMember:
    ownership = ToolOwnership.REFERENCE if relations is not None else ToolOwnership.DEFINE
    return ToolMember(
        name, kind, ownership, Origin("tools/t.yml"), "tools/t.yml", relations=MappingProxyType(relations or {})
    )


CATALOG = ToolCatalog(
    (
        _member("weekly_exports", "stage"),
        _member("partner_files", "stage", relations={"dev": "PARTNER.SHARED.FILES"}),
        _member("elsewhere", "stage", relations={"prod": "PARTNER.SHARED.FILES"}),
        _member("docs_search", "cortex_search_service"),
    ),
    "dev",
    frozenset(("dev", "prod")),
)


def _defined_at(member: ToolMember) -> QualifiedName:
    return QualifiedName.from_parts("DEV_DB", "TOOLS", member.name)


def _stage(text: str) -> str | Diagnostic:
    return resolve_stage_path(text, CATALOG, _defined_at, ORIGIN, field="docs.stage_path", artifact="a")


def test_a_stage_path_follows_the_member_it_opens_with_and_an_external_one_is_kept() -> None:
    defined = _defined_at(CATALOG.members[0]).sql
    assert _stage("{{ tool('weekly_exports') }}/2026/") == f"@{defined}/2026/"
    assert _stage("{{ tool('weekly_exports') }}") == f"@{defined}"
    assert _stage("  {{ tool('partner_files') }}/in/") == "@PARTNER.SHARED.FILES/in/"
    assert _stage("@EXT.SHARED.DROPS/in/") == "@EXT.SHARED.DROPS/in/"


@pytest.mark.parametrize(
    ("text", "code"),
    [
        ("{{ tool('ghost') }}/x", "SST-REF010"),
        ("{{ tool('docs_search') }}/x", "SST-REF020"),
        ("{{ tool('elsewhere') }}/x", "SST-REF018"),
        ("@DB.S.{{ tool('weekly_exports') }}", "SST-REF003"),
        ("{{ tool('weekly_exports') }}/{{ tool('partner_files') }}", "SST-REF003"),
        ("{{ ref('weekly_exports') }}/x", "SST-REF041"),
        ("{{ tool('weekly_exports'", "SST-LOD004"),
    ],
)
def test_a_stage_path_that_does_not_name_a_usable_stage_is_reported(text: str, code: str) -> None:
    assert _code(_stage(text)) == code


@pytest.mark.parametrize(
    ("text", "member"),
    [
        ("{{ tool('weekly_exports') }}/2026/", "weekly_exports"),
        ("@DB.S.EXTERNAL/2026/", None),
        ("{{ tool('a'", None),
        ("{{ ref('a') }}/x", None),
        ("{{ tool('a', 'b') }}/x", None),
        ("x/{{ tool('a') }}", None),
        ("{{ tool('a') }}{{ tool('b') }}", None),
    ],
)
def test_a_stage_path_names_its_member_only_when_it_opens_with_one_tool_call(text: str, member: str | None) -> None:
    assert stage_member(text) == member


def test_declared_tags_qualify_each_key_by_its_fqn_or_the_prefix() -> None:
    block = {
        "default_prefix": "{{ target.database }}.GOVERNANCE",
        "cost_center": {"fqn": "{{ target.database }}.TAGS.COST_CENTER"},
        "tier": None,
        "a.b": {},
    }
    tags = declared_tags(block, TARGET)
    assert dict(tags.names) == {"cost_center": "DEV_DB.TAGS.COST_CENTER", "tier": "DEV_DB.GOVERNANCE.TIER"}
    assert [(name, item.code, item.context["reason"]) for name, item in tags.unqualified.items()] == [
        ("a.b", "SST-CFG036", "the key is not a single identifier")
    ]
    unprefixed = declared_tags({"tier": {}}, TARGET)
    assert unprefixed.unqualified["tier"].context["reason"] == "the block sets no default_prefix"
    assert declared_tags(None, TARGET) == DeclaredTags()


@pytest.mark.parametrize(
    ("text", "result"),
    [
        ("{{ tag('cost_center') }}", "DEV_DB.TAGS.COST_CENTER"),
        ("cost_center", "SST-REF050"),
        ("{{ tag('missing') }}", "SST-REF028"),
        ("{{ tag('a.b') }}", "SST-CFG036"),
        ("{{ skill('cost_center') }}", "SST-REF041"),
    ],
)
def test_a_tag_name_resolves_to_the_tag_it_declares(text: str, result: str) -> None:
    tags = declared_tags({"cost_center": {"fqn": "DEV_DB.TAGS.COST_CENTER"}, "a.b": {}}, TARGET)
    found = tag_reference(
        text, tags, ORIGIN, reference="agent.config.tags.name", field="config.tags[0].name", artifact="agent:a"
    )
    assert (_code(found) or found) == result


def test_a_column_entry_that_is_a_one_argument_ref_names_no_column() -> None:
    view = ParsedView("v", ORIGIN, "views.yml", {"columns": ["{{ ref('orders') }}"]}, ("orders",))
    found = scope_diagnostics((view,), (), (), MODELS, ViewMembers({}, {}, frozenset()))
    assert [(item.code, item.context.get("reason")) for item in found] == [
        ("SST-VAL330", "is not a two-argument ref() call")
    ]
