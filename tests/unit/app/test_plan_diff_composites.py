"""The live content of a skill, plugin, profile or eval a planned update replaces, and when it cannot be read."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.compile.profiles import CompiledProfile
from snowflake_semantic_tools.app.compile.skills import CompiledExtension
from snowflake_semantic_tools.app.plan_diff_composites import (
    eval_sides,
    extension_sides,
    profile_sides,
    served_extension,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, Change, ChangeReason, CompositeObservation
from snowflake_semantic_tools.domain.ports.snowflake.catalog import ExtensionObservation, ExtensionVersion
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.plan_codes import view
from tests.helpers.snowflake_fake import FakeSnowflake

_NAME = QualifiedName.parse("DB.S.SKILL")
_CHANGE = Change("eval:a", "eval", Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS, view("a"), None, (), 0)


def _stand_in(kind: type, **fields: object) -> object:
    """An instance of a frozen compiled artifact class holding only the fields a read uses."""
    item: object = object.__new__(kind)
    for name, value in fields.items():
        object.__setattr__(item, name, value)
    return item


def _version(name: str, alias: str | None = None, *, default: bool = False) -> ExtensionVersion:
    return ExtensionVersion(name, alias, f"snow://cortex_extension/DB.S.SKILL/versions/{name.lower()}/", default)


def test_the_served_version_is_the_effective_one_else_the_default_else_the_last() -> None:
    first, second, third = (
        _version("VERSION$1", "A1"),
        _version("VERSION$2", "A2", default=True),
        _version("VERSION$10"),
    )
    observed = ExtensionObservation(_NAME, "SKILL", None, "OWNER", effective_version="VERSION$1")
    assert served_extension(observed, (first, second, third)) is first
    by_alias = ExtensionObservation(_NAME, "SKILL", None, "OWNER", effective_version="a2")
    assert served_extension(by_alias, (first, second, third)) is second
    assert served_extension(None, (first, second, third)) is second
    assert served_extension(None, (first, _version("live"), third)) is third
    assert served_extension(None, ()) is None


def test_reads_with_nothing_to_compare_say_so() -> None:
    port = FakeSnowflake()
    with pytest.raises(TypeError, match="CompiledExtension"):
        extension_sides(port, SimpleNamespace(artifact_key="skill:a"), _CHANGE)
    with pytest.raises(TypeError, match="CompiledProfile"):
        profile_sides(port, SimpleNamespace(artifact_key="profile:a"), _CHANGE)
    with pytest.raises(TypeError, match="CompiledEval"):
        eval_sides(port, SimpleNamespace(artifact_key="eval:a"), _CHANGE, None)
    release = SimpleNamespace(target=_NAME, comment=None, bundle=SimpleNamespace(entries=()))
    port.extensions[_NAME.sql] = {"type": "SKILL", "comment": None, "versions": [], "live": None}
    with pytest.raises(SnowflakePortError, match="serves no version to compare"):
        extension_sides(port, _stand_in(CompiledExtension, release=release), _CHANGE)  # type: ignore[arg-type]
    profile = _stand_in(
        CompiledProfile,
        release=SimpleNamespace(name="team", trees=(), row={}),
        channel=SimpleNamespace(registry=QualifiedName.parse("DB.S.REGISTRY")),
    )
    with pytest.raises(SnowflakePortError, match="holds no row for profile team"):
        profile_sides(port, profile, _CHANGE)  # type: ignore[arg-type]


def _eval() -> object:
    payload = json.dumps([{"input_query": "Q?", "ground_truth": {"answer": "1"}}])
    return _stand_in(CompiledEval, rendered=SimpleNamespace(config_yaml="a: 2\n", dataset_payload=payload))


def _entry(*, config: str | None = "c0ffee", table: bool = True) -> AppliedEntry:
    return AppliedEntry(
        "f",
        "DB.S.EVAL",
        "now",
        "run",
        "applied",
        "f",
        "m",
        component_fingerprints=(("config", config),) if config else (),
        physical_resources=(("TABLE", "DB.S.SRC_OLD"), ("DATASET", "DB.S.DS")) if table else (("DATASET", "DB.S.DS"),),
    )


def _eval_change() -> Change:
    observation = CompositeObservation("eval:a", config_path="@DB.S.CONFIGS/agent/feed.yaml")
    return Change(
        "eval:a",
        "eval",
        Action.UPDATE,
        ChangeReason.FINGERPRINT_DIFFERS,
        view("a"),
        None,
        (),
        0,
        composite_observation=observation,
    )


def test_an_eval_without_its_earlier_publish_in_state_is_not_compared() -> None:
    port = FakeSnowflake()
    with pytest.raises(SnowflakePortError, match="no earlier publish"):
        eval_sides(port, _eval(), _eval_change(), None)  # type: ignore[arg-type]
    with pytest.raises(SnowflakePortError, match="no config the eval published"):
        eval_sides(port, _eval(), _eval_change(), _entry(config=None))  # type: ignore[arg-type]
    with pytest.raises(SnowflakePortError, match="could not be read"):
        eval_sides(port, _eval(), _eval_change(), _entry())  # type: ignore[arg-type]


def test_an_eval_whose_source_table_state_does_not_record_compares_its_config_alone() -> None:
    port = FakeSnowflake()
    port.staged_file_contents["@DB.S.CONFIGS/agent/c0ffee.yaml"] = b"a: 1\n"
    live, desired = eval_sides(port, _eval(), _eval_change(), _entry(table=False))  # type: ignore[arg-type]
    assert [member.label if hasattr(member, "label") else member.kind for member in live.members] == ["config"]
    assert [member.kind for member in desired.members] == ["config"]
