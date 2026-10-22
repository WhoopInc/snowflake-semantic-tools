"""The live content of a skill, plugin, profile or eval a planned update replaces, and when it cannot be read."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from snowflake_semantic_tools.app.compile.coco_desktop_profiles import CompiledCocoDesktopProfile
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.plan_diff_composites import (
    desired_eval,
    desired_extension,
    live_eval,
    live_extension,
    live_profile,
    served_extension,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.plan.member_diff import diff_definitions
from snowflake_semantic_tools.domain.ports.snowflake.catalog import ExtensionObservation, ExtensionVersion
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.snowflake_fake import FakeSnowflake

_NAME = QualifiedName.parse("DB.S.SKILL")


def _stand_in(kind: type, **fields: object) -> Any:
    """An instance of a frozen compiled artifact class holding only the fields a read uses."""
    item: Any = object.__new__(kind)
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
        desired_extension(SimpleNamespace(artifact_key="skill:a"))
    with pytest.raises(TypeError, match="CompiledCocoDesktopProfile"):
        live_profile(port, SimpleNamespace(artifact_key="coco_desktop_profile:a"))
    with pytest.raises(TypeError, match="CompiledEval"):
        desired_eval(SimpleNamespace(artifact_key="eval:a"))
    port.extensions[_NAME.sql] = {"type": "SKILL", "comment": None, "versions": [], "live": None}
    with pytest.raises(SnowflakePortError, match="serves no version to compare"):
        live_extension(port, _NAME)
    profile = _stand_in(
        CompiledCocoDesktopProfile,
        release=SimpleNamespace(name="team", trees=(), row={}),
        channel=SimpleNamespace(registry=QualifiedName.parse("DB.S.REGISTRY")),
    )
    with pytest.raises(SnowflakePortError, match="holds no row for profile team"):
        live_profile(port, profile)


def _published(name: str, alias: str, path: str, *, default: bool = False) -> dict[str, object]:
    """One committed version as the fake keeps it, holding one file staged at `@S/<path>`."""
    location = f"snow://cortex_extension/DB.S.SKILL/versions/{name.lower()}/"
    return {
        "name": name,
        "alias": alias,
        "location": location,
        "files": [path],
        "sources": {path: f"@S/{path}"},
        "is_default": default,
        "certification": None,
    }


def _recorded(alias: str) -> AppliedEntry:
    return AppliedEntry("f", _NAME.sql, "now", "run", "applied", "f", "m", component_fingerprints=(("alias", alias),))


def test_an_extension_is_read_at_the_alias_state_records_else_at_the_version_served() -> None:
    port = FakeSnowflake()
    versions = [_published("VERSION$1", "A1", "one.md"), _published("VERSION$2", "A2", "two.md", default=True)]
    port.extensions[_NAME.sql] = {"type": "SKILL", "comment": "c", "versions": versions, "live": None}
    port.staged_file_contents.update({"@S/one.md": b"1", "@S/two.md": b"2"})

    def files(recorded: AppliedEntry | None) -> list[str]:
        return [member.name for member in live_extension(port, _NAME, recorded).members if member.kind == "file"]

    assert files(_recorded("a1")) == ["one.md"]
    # Nothing recorded, or an alias no version carries: the version the catalog serves.
    assert files(None) == ["two.md"]
    assert files(_recorded("GONE")) == ["two.md"]


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


_FOLDER = "@DB.S.CONFIGS/agent"


def test_an_eval_without_its_earlier_publish_in_state_is_not_compared() -> None:
    port = FakeSnowflake()
    with pytest.raises(SnowflakePortError, match="no earlier publish"):
        live_eval(port, _FOLDER, None)
    with pytest.raises(SnowflakePortError, match="no config the eval published"):
        live_eval(port, _FOLDER, _entry(config=None))
    with pytest.raises(SnowflakePortError, match="no config the eval published"):
        live_eval(port, None, _entry())
    with pytest.raises(SnowflakePortError, match="could not be read"):
        live_eval(port, _FOLDER, _entry())


def test_an_eval_whose_source_table_state_does_not_record_compares_its_config_alone() -> None:
    port = FakeSnowflake()
    port.staged_file_contents[f"{_FOLDER}/c0ffee.yaml"] = b"a: 1\n"
    live = live_eval(port, _FOLDER, _entry(table=False))
    assert ([member.kind for member in live.members], live.kinds) == (["config"], frozenset(("config",)))
    payload = json.dumps([{"input_query": "Q?", "ground_truth": {"answer": "1"}}])
    desired = desired_eval(
        _stand_in(CompiledEval, rendered=SimpleNamespace(config_yaml="a: 2\n", dataset_payload=payload))
    )
    # The questions the live side did not read are not shown as added; the config's change is.
    assert [(item.op, item.label) for item in diff_definitions(live, desired)] == [("changed", "config yaml")]
