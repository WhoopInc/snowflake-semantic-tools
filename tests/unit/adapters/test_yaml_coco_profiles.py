"""`load_coco_profile_catalog` reads Cortex Code agent profile folders, a Snowflake private preview artifact.

Each folder under `coco_profiles/` is one profile, except `shared/`, the layer every profile
carries. A folder that cannot be a profile is reported and left out; a linked one is refused.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml import coco_profiles
from snowflake_semantic_tools.adapters.yaml.coco_profiles import load_coco_profile_catalog
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.coco_profile import REJECTED_COCO_PROFILE_KEYS, CocoProfileCatalog
from tests.helpers.diagnostic_filters import codes, only

MANIFEST = "coco_profiles/analyst/coco_profile.yml"


def _write(root: Path, files: dict[str, str | bytes]) -> Path:
    for name, value in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(value, bytes):
            path.write_bytes(value)
        else:
            path.write_text(value, encoding="utf-8")
    return root


def _load(
    root: Path, files: dict[str, str | bytes], config: dict[str, Any] | None = None, **kwargs: str
) -> tuple[CocoProfileCatalog, DiagnosticBag]:
    return load_coco_profile_catalog(_write(root, files), config=config, **kwargs)


def test_a_complete_profile_reads_every_field_and_resolves_its_settings(tmp_path: Path) -> None:
    manifest = (
        "name: analyst\n"
        "description: Analysts.\n"
        "model: claude-sonnet\n"
        "skills:\n  - \"{{ skill('month-close') }}\"\n"
        "plugins:\n  - \"{{ plugin('finance-kit') }}\"\n"
        "config:\n  name_suffix: _dev\n"
    )
    config: dict[str, Any] = {"coco_profiles": {"+database": "CORTEX_CODE", "analyst": {"+schema": "CONFIG"}}}
    catalog, diagnostics = _load(
        tmp_path, {MANIFEST: manifest, "coco_profiles/analyst/AGENTS.md": "Be careful.\n"}, config
    )

    assert list(diagnostics) == []
    [model] = catalog.profiles
    assert (model.name, model.key, model.comment, model.orchestration_model, model.prompt) == (
        "analyst",
        "coco_profile:analyst",
        "Analysts.",
        "claude-sonnet",
        "Be careful.\n",
    )
    assert [(skill.ref, skill.path, skill.source_type) for skill in model.skills] == [
        ("skill", "month-close", "CORTEX_EXTENSION"),
        ("plugin", "finance-kit", "CORTEX_EXTENSION"),
    ]
    assert model.origin == Origin(MANIFEST, 1)
    assert model.source_files == (MANIFEST, "coco_profiles/analyst/AGENTS.md")
    assert {key: (item.value, item.source) for key, item in model.settings.items()} == {
        "database": ("CORTEX_CODE", "root"),
        "schema": ("CONFIG", "route:analyst"),
        "name_suffix": ("_dev", "config"),
    }
    assert model.enabled and model.published_name == "analyst"
    assert catalog.shared is None


def test_a_profile_without_a_prompt_or_entries_and_one_disabled_by_its_route(tmp_path: Path) -> None:
    files: dict[str, str | bytes] = {MANIFEST: "{}\n", "coco_profiles/operator/coco_profile.yaml": "{}\n"}
    catalog, diagnostics = _load(tmp_path, files, {"coco_profiles": {"operator": {"+enabled": False}}})

    assert list(diagnostics) == []
    analyst, operator = catalog.profiles
    assert (analyst.prompt, analyst.skills, analyst.source_files, analyst.enabled) == (None, (), (MANIFEST,), True)
    assert operator.source_files == ("coco_profiles/operator/coco_profile.yaml",) and not operator.enabled


def test_without_a_configuration_a_profile_resolves_its_own_config_alone(tmp_path: Path) -> None:
    catalog, _ = _load(tmp_path, {MANIFEST: "config:\n  enabled: false\n"})
    assert not catalog.profiles[0].enabled
    assert {key: item.source for key, item in catalog.profiles[0].settings.items()} == {"enabled": "config"}


def test_the_shared_layer_reads_its_skills_prompt_and_published_rules_in_name_order(tmp_path: Path) -> None:
    files: dict[str, str | bytes] = {
        "coco_profiles/shared/coco_profile.yml": "skills:\n  - \"{{ skill('sql-author') }}\"\n",
        "coco_profiles/shared/AGENTS.md": "Shared.\n",
        "coco_profiles/shared/rules/b.md": "Rule b.\n",
        "coco_profiles/shared/rules/a.md": "Rule a.\n",
        "coco_profiles/shared/rules/.draft.md": "Hidden.\n",
        "coco_profiles/shared/rules/notes.txt": "Not a rule.\n",
        "coco_profiles/shared/rules/broken.md": b"\xff",
        MANIFEST: "{}\n",
    }
    (tmp_path / "coco_profiles/shared/rules/folder.md").mkdir(parents=True)
    catalog, diagnostics = _load(tmp_path, files)

    assert codes(diagnostics) == ["SST-LOD006"]
    shared = catalog.shared
    assert shared is not None
    assert (shared.prompt, shared.skills, shared.rules) == (
        "Shared.\n",
        ("sql-author",),
        (("a.md", "Rule a.\n"), ("b.md", "Rule b.\n")),
    )
    assert shared.origin == Origin("coco_profiles/shared/coco_profile.yml", 1)
    assert shared.source_files == (
        "coco_profiles/shared/coco_profile.yml",
        "coco_profiles/shared/AGENTS.md",
        "coco_profiles/shared/rules/a.md",
        "coco_profiles/shared/rules/b.md",
    )
    # `shared/` is the layer every profile carries, never a profile itself.
    assert [model.name for model in catalog.profiles] == ["analyst"]


def test_a_shared_layer_without_a_manifest_lists_no_skills(tmp_path: Path) -> None:
    catalog, diagnostics = _load(tmp_path, {"coco_profiles/shared/AGENTS.md": "Shared.\n"})
    assert list(diagnostics) == [] and catalog.profiles == ()
    assert catalog.shared is not None
    assert (catalog.shared.skills, catalog.shared.origin) == ((), Origin("coco_profiles/shared"))
    assert catalog.shared.source_files == ("coco_profiles/shared/AGENTS.md",)


def test_a_shared_layer_with_both_spellings_an_unknown_key_or_a_broken_manifest_lists_no_skills(
    tmp_path: Path,
) -> None:
    both, diagnostics = _load(
        tmp_path / "both",
        {"coco_profiles/shared/coco_profile.yml": "{}\n", "coco_profiles/shared/coco_profile.yaml": "{}\n"},
    )
    found = only(diagnostics, "SST-VAL801")
    assert found.subject == "coco_profile:shared" and "has both" in found.message
    assert both.shared is not None and both.shared.skills == ()

    _, unknown = _load(tmp_path / "unknown", {"coco_profiles/shared/coco_profile.yml": "hooks: []\n"})
    assert only(unknown, "SST-PRS004").subject == "coco_profile:shared"

    broken, unparsed = _load(tmp_path / "broken", {"coco_profiles/shared/coco_profile.yml": "a: [\n"})
    assert codes(unparsed) == ["SST-LOD001"] and broken.shared is not None and broken.shared.skills == ()


def test_a_leftover_desktop_profiles_folder_is_val895_unless_it_is_the_profiles_directory(tmp_path: Path) -> None:
    (tmp_path / "coco_desktop_profiles").mkdir()
    _, diagnostics = load_coco_profile_catalog(tmp_path)
    assert only(diagnostics, "SST-VAL895").origin == Origin("coco_desktop_profiles")

    _, reused = load_coco_profile_catalog(tmp_path, coco_profiles_dir="coco_desktop_profiles")
    assert list(reused) == []


def test_a_folder_route_that_names_no_folder_is_cfg041(tmp_path: Path) -> None:
    _, diagnostics = _load(tmp_path, {MANIFEST: "{}\n"}, {"coco_profiles": {"missing": {"+schema": "X"}}})
    assert only(diagnostics, "SST-CFG041").subject == "config_route:missing"


def test_a_missing_directory_hidden_folders_caches_and_loose_files_hold_no_profile(tmp_path: Path) -> None:
    assert load_coco_profile_catalog(tmp_path) == (CocoProfileCatalog((), None), DiagnosticBag())
    files: dict[str, str | bytes] = {
        "coco_profiles/.hidden/coco_profile.yml": "{}\n",
        "coco_profiles/__pycache__/coco_profile.yml": "{}\n",
        "coco_profiles/README.md": "Notes.\n",
    }
    catalog, diagnostics = _load(tmp_path, files)
    assert catalog == CocoProfileCatalog((), None) and list(diagnostics) == []


@pytest.mark.parametrize(
    ("files", "detail"),
    [
        ({"coco_profiles/Analyst/coco_profile.yml": "{}\n"}, "is not lowercase letters"),
        ({"coco_profiles/analyst/AGENTS.md": "Hi.\n"}, "profile folder has no coco_profile.yml"),
        (
            {MANIFEST: "{}\n", "coco_profiles/analyst/coco_profile.yaml": "{}\n"},
            "profile folder has both coco_profile.yml and coco_profile.yaml",
        ),
    ],
    ids=["bad name", "no manifest", "both spellings"],
)
def test_a_folder_that_cannot_be_a_profile_is_val801_and_left_out(
    tmp_path: Path, files: dict[str, str | bytes], detail: str
) -> None:
    catalog, diagnostics = _load(tmp_path, files)
    assert catalog.profiles == ()
    assert detail in only(diagnostics, "SST-VAL801").message


def test_a_declared_name_that_is_not_the_folders_is_val801_and_the_profile_keeps_the_folder_name(
    tmp_path: Path,
) -> None:
    catalog, diagnostics = _load(tmp_path, {MANIFEST: "name: other\n"})
    found = only(diagnostics, "SST-VAL801")
    assert "does not match the folder name" in found.message and found.origin == Origin(MANIFEST, 1)
    assert [model.name for model in catalog.profiles] == ["analyst"]


def test_each_key_a_profile_cannot_carry_is_val851_naming_why_and_any_other_is_prs004(tmp_path: Path) -> None:
    rejected = "".join(f"{key}: x\n" for key in REJECTED_COCO_PROFILE_KEYS)
    catalog, diagnostics = _load(tmp_path, {MANIFEST: rejected + "colour: blue\n"})

    refused = [item for item in diagnostics if item.code == "SST-VAL851"]
    assert sorted(item.context["key"] for item in refused) == sorted(REJECTED_COCO_PROFILE_KEYS)
    assert {item.context["key"]: item.context["reason"] for item in refused} == dict(REJECTED_COCO_PROFILE_KEYS)
    assert only(diagnostics, "SST-PRS004").context["field"] == "colour"
    assert [model.name for model in catalog.profiles] == ["analyst"]


def test_a_config_block_is_checked_and_fields_of_the_wrong_shape_are_prs003(tmp_path: Path) -> None:
    manifest = "description: 3\nmodel: [1]\nskills: month-close\nconfig:\n  colour: blue\n"
    catalog, diagnostics = _load(tmp_path, {MANIFEST: manifest})

    assert codes(diagnostics).count("SST-PRS003") == 3
    assert only(diagnostics, "SST-CFG053").context["key"] == "config.colour"
    [model] = catalog.profiles
    assert (model.comment, model.orchestration_model, model.skills) == (None, None, ())


@pytest.mark.parametrize(
    ("text", "code"),
    [("a: [\n", "SST-LOD001"), ("- a list\n", "SST-LOD002"), ("a: 1\na: 2\n", "SST-LOD005")],
    ids=["syntax", "not a mapping", "duplicate key"],
)
def test_a_manifest_that_does_not_parse_leaves_the_profile_out(tmp_path: Path, text: str, code: str) -> None:
    catalog, diagnostics = _load(tmp_path, {MANIFEST: text})
    assert catalog.profiles == ()
    assert codes(diagnostics) == [code] and diagnostics[0].subject == "coco_profile:analyst"


def test_a_parse_refusal_that_carries_no_diagnostic_is_reported_as_lod001(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(data: bytes, file: str) -> object:
        error = ProjectError("unreadable", diagnostics=(D("SST-INT010", detail="x"),))
        error.diagnostics = ()
        raise error

    monkeypatch.setattr(coco_profiles, "parse_yaml_bytes", refuse)
    catalog, diagnostics = _load(tmp_path, {MANIFEST: "{}\n"})
    found = only(diagnostics, "SST-LOD001")
    assert catalog.profiles == () and (found.subject, found.origin) == ("coco_profile:analyst", Origin(MANIFEST))


def test_an_agents_md_that_is_not_utf8_is_refused_and_the_profile_kept_without_a_prompt(tmp_path: Path) -> None:
    catalog, diagnostics = _load(tmp_path, {MANIFEST: "{}\n", "coco_profiles/analyst/AGENTS.md": b"\xff"})
    assert codes(diagnostics) == ["SST-LOD006"]
    assert catalog.profiles[0].prompt is None


def test_a_linked_folder_manifest_or_prompt_is_prt009_and_never_read(tmp_path: Path) -> None:
    real = _write(
        tmp_path / "real",
        {"coco_profile.yml": "description: Linked.\n", "AGENTS.md": "Linked.\n", "rules/a.md": "Linked rule.\n"},
    )
    _write(tmp_path, {"coco_profiles/operator/AGENTS.md": "x\n", "coco_profiles/shared/AGENTS.md": "Shared.\n"})
    (tmp_path / "coco_profiles/linked").symlink_to(real, target_is_directory=True)
    (tmp_path / "coco_profiles/operator/coco_profile.yml").symlink_to(real / "coco_profile.yml")
    (tmp_path / "coco_profiles/viewer").mkdir()
    (tmp_path / "coco_profiles/viewer/coco_profile.yml").write_text("{}\n", encoding="utf-8")
    (tmp_path / "coco_profiles/viewer/AGENTS.md").symlink_to(real / "AGENTS.md")
    (tmp_path / "coco_profiles/shared/rules").mkdir()
    (tmp_path / "coco_profiles/shared/rules/a.md").symlink_to(real / "rules/a.md")

    catalog, diagnostics = load_coco_profile_catalog(tmp_path)

    refused = sorted(str(item.context["path"]) for item in diagnostics if item.code == "SST-PRT009")
    assert refused == [
        "coco_profiles/linked",
        "coco_profiles/operator/coco_profile.yml",
        "coco_profiles/shared/rules/a.md",
        "coco_profiles/viewer/AGENTS.md",
    ]
    assert [(model.name, model.prompt) for model in catalog.profiles] == [("viewer", None)]
    assert catalog.shared is not None and catalog.shared.rules == ()
