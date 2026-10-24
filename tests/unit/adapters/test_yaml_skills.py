"""Skill folders and plugin manifests load from disk with located diagnostics."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.skills import load_skill_catalog
from snowflake_semantic_tools.domain.diagnostics import Origin
from tests.helpers.file_trees import write_tree

GOOD = "---\nname: {name}\ndescription: Close the month.\n---\n# Steps\nRead reference/a.md.\n"


def test_discovers_grouped_and_nested_skills_and_skips_hidden_caches(tmp_path: Path) -> None:
    write_tree(
        tmp_path,
        {
            "skills/finance/month-close/SKILL.md": GOOD.format(name="month-close"),
            "skills/finance/month-close/reference/a.md": "a",
            "skills/finance/month-close/.DS_Store": "x",
            "skills/finance/month-close/scripts/__pycache__/x.pyc": "x",
            "skills/finance/month-close/scripts/run.pyc": "x",
            "skills/finance/month-close/inner/SKILL.md": GOOD.format(name="inner"),
            "skills/finance/month-close/inner/notes.md": "n",
            "skills/.hidden/secret/SKILL.md": GOOD.format(name="secret"),
            "skills/top/SKILL.md": GOOD.format(name="top"),
        },
    )
    catalog = load_skill_catalog(tmp_path, skills_dir="skills", plugins_dir="plugins")
    assert [skill.name for skill in catalog.skills] == ["month-close", "inner", "top"]
    month, inner, _ = catalog.skills
    # A nested skill is a skill of its own, and still part of the folder that holds it.
    assert [item.path for item in month.files] == ["SKILL.md", "inner/SKILL.md", "inner/notes.md", "reference/a.md"]
    assert month.nested == ("inner",)
    assert [item.path for item in inner.files] == ["SKILL.md", "notes.md"]
    assert (inner.directory, inner.folder, inner.nested) == (
        "skills/finance/month-close/inner",
        ("finance", "month-close", "inner"),
        (),
    )
    assert (month.directory, month.declared_name, month.description) == (
        "skills/finance/month-close",
        "month-close",
        "Close the month.",
    )
    assert month.body == "# Steps\nRead reference/a.md.\n"
    assert month.origin == Origin("skills/finance/month-close/SKILL.md", 1)
    assert catalog.diagnostics == ()


def test_frontmatter_defects_are_located(tmp_path: Path) -> None:
    write_tree(
        tmp_path,
        {
            "skills/none/SKILL.md": "# No frontmatter\n",
            "skills/broken/SKILL.md": "---\nname: broken\ndescription: [unclosed\n---\nbody\n",
            "skills/listy/SKILL.md": "---\n- a\n---\nbody\n",
            "skills/empty/SKILL.md": "---\n---\nbody\n",
            "skills/nodesc/SKILL.md": "---\nname: nodesc\ndescription: '  '\n---\nbody\n",
            "skills/binary/SKILL.md": b"\xff\xfe",
        },
    )
    catalog = load_skill_catalog(tmp_path, skills_dir="skills", plugins_dir="plugins")
    found = [(item.subject, item.code, item.context.get("field")) for item in catalog.diagnostics]
    assert found == [
        ("skill:binary", "SST-LOD006", None),
        ("skill:broken", "SST-LOD001", None),
        ("skill:empty", "SST-PRS034", "name"),
        ("skill:empty", "SST-PRS034", "description"),
        ("skill:listy", "SST-LOD002", None),
        ("skill:nodesc", "SST-PRS034", "description"),
        ("skill:none", "SST-PRS034", "name"),
        ("skill:none", "SST-PRS034", "description"),
    ]
    broken = next(item for item in catalog.diagnostics if item.subject == "skill:broken")
    assert broken.origin is not None and broken.origin.line == 4
    assert catalog.skill("none") is not None and catalog.skill("none").body == "# No frontmatter\n"  # type: ignore[union-attr]


def test_missing_directories_yield_an_empty_catalog(tmp_path: Path) -> None:
    catalog = load_skill_catalog(tmp_path, skills_dir="skills", plugins_dir="plugins")
    assert (catalog.skills, catalog.plugins, catalog.diagnostics) == ((), (), ())


def test_plugin_manifests_are_parsed_and_checked(tmp_path: Path) -> None:
    write_tree(
        tmp_path,
        {
            "plugins/finance-kit/plugin.yml": (
                "name: finance-kit\ndescription: Finance.\nowner_team: Analytics\n"
                "skills:\n  - \"{{ skill('month-close') }}\"\n"
            ),
            "plugins/odd/plugin.yml": "name: other\ndescription: 3\nowner_team: [a]\nskills: nope\nextra: 1\n",
            "plugins/bare/plugin.yaml": "name: bare\n",
            "plugins/twice/plugin.yml": "name: twice\n",
            "plugins/twice/plugin.yaml": "name: twice\n",
            "plugins/empty/README.md": "no manifest",
            "plugins/bad/plugin.yml": "name: [unclosed\n",
            "plugins/.hidden/plugin.yml": "name: hidden\n",
        },
    )
    catalog = load_skill_catalog(tmp_path, skills_dir="skills", plugins_dir="plugins")
    assert [plugin.name for plugin in catalog.plugins] == ["bare", "finance-kit", "odd"]
    kit = catalog.plugins[1]
    assert (kit.description, kit.owner_team, kit.members, kit.manifest_file) == (
        "Finance.",
        "Analytics",
        ("month-close",),
        "plugins/finance-kit/plugin.yml",
    )
    assert catalog.plugins[0].members == ()
    found = [(item.subject, item.code, item.context.get("field")) for item in catalog.diagnostics]
    assert found == [
        ("plugin:bad", "SST-LOD001", None),
        ("plugin:bare", "SST-PRS002", "description"),
        ("plugin:empty", "SST-VAL801", None),
        ("plugin:odd", "SST-PRS004", "extra"),
        ("plugin:odd", "SST-VAL801", None),
        ("plugin:odd", "SST-PRS003", "description"),
        ("plugin:odd", "SST-PRS003", "owner_team"),
        ("plugin:odd", "SST-PRS002", "description"),
        ("plugin:odd", "SST-PRS003", "skills"),
        ("plugin:twice", "SST-VAL801", None),
    ]


def test_linked_skills_files_folders_and_plugins_are_refused(tmp_path: Path) -> None:
    project = tmp_path / "project"
    outside = write_tree(
        tmp_path / "outside",
        {
            "skill/SKILL.md": GOOD.format(name="linked"),
            "SKILL.md": GOOD.format(name="file"),
            "plugin/plugin.yml": "description: Outside.\n",
            "plugin.yml": "description: Outside.\n",
        },
    )
    write_tree(project, {"skills/kept/SKILL.md": GOOD.format(name="kept"), "plugins/local/README.md": "x"})
    (project / "skills/linked").symlink_to(outside / "skill")
    (project / "skills/file").mkdir()
    (project / "skills/file/SKILL.md").symlink_to(outside / "SKILL.md")
    (project / "plugins/linked").symlink_to(outside / "plugin")
    (project / "plugins/local/plugin.yml").symlink_to(outside / "plugin.yml")
    catalog = load_skill_catalog(project, skills_dir="skills", plugins_dir="plugins")
    assert [skill.name for skill in catalog.skills] == ["kept"]
    assert catalog.plugins == ()
    assert [(item.code, item.context["path"]) for item in catalog.diagnostics] == [
        ("SST-PRT009", "skills/file/SKILL.md"),
        ("SST-PRT009", "skills/linked"),
        ("SST-PRT009", "plugins/linked"),
        ("SST-PRT009", "plugins/local/plugin.yml"),
    ]


def test_a_skills_root_reached_through_a_link_inside_the_project_is_refused(tmp_path: Path) -> None:
    write_tree(tmp_path, {"real/kept/SKILL.md": GOOD.format(name="kept")})
    (tmp_path / "skills").symlink_to(tmp_path / "real")
    catalog = load_skill_catalog(tmp_path, skills_dir="skills", plugins_dir="plugins")
    assert catalog.skills == ()
    assert {item.message for item in catalog.diagnostics} == {
        "could not read skills/kept: skills is a symbolic link, which SST does not follow",
        "could not read skills/kept/SKILL.md: skills is a symbolic link, which SST does not follow",
    }


def test_each_item_records_its_folder_and_its_own_settings_and_each_route_is_checked(tmp_path: Path) -> None:
    owned = "---\nname: close\ndescription: Close.\nconfig:\n  certified: true\n  schema: FIN\n---\nBody.\n"
    write_tree(
        tmp_path,
        {
            "skills/finance/close/SKILL.md": owned,
            "skills/top/SKILL.md": GOOD.format(name="top"),
            # A grouping folder holds only plugin folders; a leaf without a manifest is still one.
            "plugins/finance/kit/plugin.yml": (
                "name: kit\ndescription: Kit.\nskills: [\"{{ skill('close') }}\"]\nconfig:\n  version_prefix: KIT_\n"
            ),
            "plugins/finance/empty/notes.txt": "x",
            "plugins/solo/plugin.yml": "name: solo\ndescription: Solo.\nskills: [\"{{ skill('top') }}\"]\n",
        },
    )
    config = {"skills": {"finance": {"+schema": "F"}, "missing": {}}, "plugins": {"finance": {"kit": {}}}}
    catalog = load_skill_catalog(tmp_path, skills_dir="skills", plugins_dir="plugins", config=config)

    assert [(skill.name, skill.folder, dict(skill.config)) for skill in catalog.skills] == [
        ("close", ("finance", "close"), {"certified": True, "schema": "FIN"}),
        ("top", ("top",), {}),
    ]
    assert [(item.name, item.folder, dict(item.config)) for item in catalog.plugins] == [
        ("kit", ("finance", "kit"), {"version_prefix": "KIT_"}),
        ("solo", ("solo",), {}),
    ]
    assert [(item.code, item.subject) for item in catalog.diagnostics] == [
        ("SST-CFG041", "config_route:missing"),
        ("SST-VAL801", "plugin:empty"),
    ]


def test_an_items_own_settings_are_checked_against_its_block(tmp_path: Path) -> None:
    bad = "---\nname: close\ndescription: Close.\nconfig:\n  threads: 2\n  stage: S\n  certified: null\n---\nBody.\n"
    write_tree(
        tmp_path,
        {
            "skills/close/SKILL.md": bad,
            "plugins/kit/plugin.yml": "name: kit\ndescription: Kit.\nskills: []\nconfig:\n  bundle_stage: 3\n",
        },
    )
    catalog = load_skill_catalog(tmp_path, skills_dir="skills", plugins_dir="plugins")
    found = [(item.code, item.subject, item.context.get("key")) for item in catalog.diagnostics]
    assert ("SST-CFG055", "skill:close", "config.threads") in found
    assert ("SST-CFG053", "skill:close", "config.stage") in found
    assert ("SST-CFG054", "skill:close", "config.certified") in found
    [mistyped] = [item for item in catalog.diagnostics if item.code == "SST-CFG004"]
    assert (mistyped.subject, mistyped.context["key"], mistyped.origin) == (
        "plugin:kit",
        "config.bundle_stage",
        Origin("plugins/kit/plugin.yml", 5, 17),
    )
