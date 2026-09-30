"""Skill folders flatten deterministically, and every path assertion is checked."""

from __future__ import annotations

import json
from dataclasses import replace

from snowflake_semantic_tools.domain.model.diagnostic import Origin
from snowflake_semantic_tools.domain.model.skill import (
    BUNDLE_BUDGET_BYTES,
    SCAN_MAX_FILE_BYTES,
    SCAN_MAX_FILES,
    SCAN_MAX_TOTAL_BYTES,
    SKILL_MD_BUDGET_BYTES,
    BundleEntry,
    Plugin,
    Skill,
    SkillBundle,
    SkillCatalog,
    SkillFile,
    build_plugin_bundle,
    build_skill_bundle,
    bundle_digest,
    extension_identifier,
    flatten_skill,
    flattened_name,
    plugin_manifest_json,
    scan_references,
    validate_skill_catalog,
)
from snowflake_semantic_tools.domain.model.skill.flatten import _recheck

SKILL_MD = "---\nname: {name}\ndescription: Does things.\n---\n{body}"


def skill(name: str = "month-close", files: dict[str, bytes | str] | None = None, **fields: object) -> Skill:
    authored = {"SKILL.md": SKILL_MD.format(name=name, body="# Close\nRead reference/steps.md.\n")}
    authored.update(files or {})
    entries = tuple(
        sorted(
            (
                SkillFile(path, value.encode("utf-8") if isinstance(value, str) else value)
                for path, value in authored.items()
            ),
            key=lambda item: item.path,
        )
    )
    values: dict[str, object] = {
        "name": name,
        "directory": f"skills/finance/{name}",
        "declared_name": name,
        "description": "Does things.",
        "body": "# Close\n",
        "files": entries,
        "origin": Origin(f"skills/finance/{name}/SKILL.md", 1),
    }
    values.update(fields)
    return Skill(**values)  # type: ignore[arg-type]


def codes(diagnostics: object) -> list[str]:
    return [item.code for item in diagnostics]  # type: ignore[attr-defined]


def test_records_expose_identity_digests_and_text() -> None:
    markdown = SkillFile("reference/a.md", b"hello")
    binary = SkillFile("assets/logo.png", b"\xff\xfe")
    assert (markdown.size, markdown.is_markdown, markdown.text) == (5, True, "hello")
    assert markdown.sha256 == BundleEntry("x", b"hello").sha256
    assert (binary.is_markdown, binary.text) == (False, None)
    authored = skill(files={"scripts/run.py": "print(1)\n", "reference/steps.md": "x"})
    assert authored.key == "skill:month-close"
    assert authored.extension_name == "MONTH_CLOSE" == extension_identifier("month-close")
    assert authored.source_files[0] == "skills/finance/month-close/SKILL.md"
    assert [item.path for item in authored.scripts] == ["scripts/run.py"]
    plugin = Plugin(
        "finance-kit", "plugins/finance-kit", "plugins/finance-kit/plugin.yml", "Kit.", None, (), Origin("p")
    )
    assert (plugin.key, plugin.extension_name) == ("plugin:finance-kit", "FINANCE_KIT")
    catalog = SkillCatalog((authored,), (plugin,))
    assert catalog.skill("month-close") is authored
    assert catalog.skill("missing") is None


def test_bundle_digest_alias_and_manifest_are_deterministic() -> None:
    entries = (BundleEntry("skills/a/b.md", b"b"), BundleEntry("skills/a/SKILL.md", b"a"))
    bundle = SkillBundle("SKILL", "a", entries, (("reference/b.md", "reference__b.md"),))
    assert bundle.digest == bundle_digest(tuple(reversed(entries)))
    assert bundle.alias("SST_") == "SST_" + bundle.digest[:12].upper()
    manifest = json.loads(bundle.manifest(alias=bundle.alias("SST_"), target="DB.S.A", comment="c"))
    assert manifest["type"] == "SKILL" and manifest["extension"] == "DB.S.A"
    assert manifest["alias"] == bundle.alias("SST_") and "certified" not in manifest
    certified = json.loads(bundle.manifest(alias="A", target="DB.S.A", comment="c", certified=True))
    assert certified["certified"] is True
    assert manifest["flattened"] == [{"authored": "reference/b.md", "published": "reference__b.md"}]
    assert flattened_name("reference/deep/x.md") == "reference__deep__x.md"
    assert flattened_name("SKILL.md") == "SKILL.md"


def test_scan_finds_every_d179_form_and_skips_the_rest() -> None:
    text = (
        "See [steps](reference/steps.md#top) and ![d](assets/d.png).\n"
        "[def]: reference/def.md\n"
        "Run reference/other.md or visit https://example.com/x.md and [a](#anchor).\n"
        "```bash\n"
        "python scripts/run.py  # sst: ignore SST-VAL808\n"
        "```\n"
        "```\nunterminated scripts/tail.sh\n"
    )
    found = scan_references(text, "SKILL.md")
    by_text = {item.text: item for item in found}
    assert by_text["reference/steps.md"].form == "link"
    assert by_text["reference/steps.md"].anchor == "#top"
    assert (by_text["reference/steps.md"].line, by_text["reference/steps.md"].col) == (1, 13)
    assert by_text["assets/d.png"].form == "image"
    assert by_text["reference/def.md"].form == "definition"
    assert by_text["reference/other.md"].form == "bare"
    assert by_text["scripts/run.py"].form == "fenced"
    assert by_text["scripts/run.py"].suppressed is True
    assert by_text["scripts/tail.sh"].form == "fenced"
    assert by_text[""].anchor == "#anchor"
    assert "example.com/x.md" not in by_text


def test_flatten_rewrites_markdown_references_and_leaves_scripts_untouched() -> None:
    authored = skill(
        files={
            "SKILL.md": SKILL_MD.format(
                name="month-close",
                body="Read [steps](reference/steps.md#s) then run scripts/run.py and ./helper.sql.\n",
            ),
            "reference/steps.md": "Back to [root](../SKILL.md) and sibling notes.md.\n",
            "reference/notes.md": "notes\n",
            "scripts/run.py": "print('run')\n",
            "helper.sql": "select 1\n",
            "assets/logo.png": b"\x89PNG\xff",
        }
    )
    files, renames, diagnostics = flatten_skill(authored)
    published = {item.path: item for item in files}
    assert sorted(published) == [
        "SKILL.md",
        "assets__logo.png",
        "helper.sql",
        "reference__notes.md",
        "reference__steps.md",
        "scripts__run.py",
    ]
    assert "[steps](reference__steps.md#s)" in (published["SKILL.md"].text or "")
    assert "scripts__run.py" in (published["SKILL.md"].text or "")
    assert "./helper.sql" in (published["SKILL.md"].text or "")
    assert "[root](SKILL.md)" in (published["reference__steps.md"].text or "")
    assert "reference__notes.md" in (published["reference__steps.md"].text or "")
    assert published["scripts__run.py"].content == b"print('run')\n"
    assert ("reference/steps.md", "reference__steps.md") in renames
    assert codes(diagnostics) == ["SST-VAL813"]
    assert diagnostics[0].context["path"] == "assets/logo.png"


def test_flatten_reports_collisions_in_authored_terms_before_anything_else() -> None:
    authored = skill(files={"a/b__c.md": "x", "a__b/c.md": "y"})
    files, renames, diagnostics = flatten_skill(authored)
    assert (files, renames) == ((), ())
    assert codes(diagnostics) == ["SST-VAL809"]
    assert {diagnostics[0].context["a"], diagnostics[0].context["b"]} == {"a/b__c.md", "a__b/c.md"}


def test_unresolved_anchored_directory_and_suppressed_references() -> None:
    body = (
        "Run scripts/check-supply-month.sql.\n"
        "Run bash .cortex/skills/month-close/scripts/x.sh.\n"
        "Open [the folder](reference/).\n"
        "Mention models/marts/orders.sql from the user's repo.\n"
        "Example scripts/example.py  sst: ignore SST-VAL808\n"
        "Read reference/steps.md.\n"
        "See [outside](../outside.md), ./missing.sh, [anchor](#x) and [site](https://example.com).\n"
    )
    authored = skill(files={"SKILL.md": SKILL_MD.format(name="month-close", body=body), "reference/steps.md": "s"})
    _, _, diagnostics = flatten_skill(authored)
    assert codes(diagnostics) == ["SST-VAL808", "SST-VAL810", "SST-VAL810", "SST-VAL808", "SST-VAL808"]
    assert [item.context["path"] for item in diagnostics] == [
        "scripts/check-supply-month.sql",
        ".cortex/skills/month-close/scripts/x.sh",
        "reference/",
        "../outside.md",
        "./missing.sh",
    ]
    assert diagnostics[0].origin == Origin("skills/finance/month-close/SKILL.md", 5, 5)


def test_repo_anchored_reference_still_counts_its_target_as_referenced() -> None:
    body = "Read skills/month-close/reference/steps.md, then skills/month-close/reference/gone.md.\n"
    authored = skill(files={"SKILL.md": SKILL_MD.format(name="month-close", body=body), "reference/steps.md": "s"})
    _, _, diagnostics = flatten_skill(authored)
    assert codes(diagnostics) == ["SST-VAL810", "SST-VAL810"]


def test_recheck_catches_a_reference_the_rewrite_missed() -> None:
    authored = skill(files={"reference/steps.md": "s"})
    unrewritten = tuple(SkillFile(flattened_name(item.path), item.content) for item in authored.files)
    diagnostics = _recheck(authored, unrewritten)
    assert codes(diagnostics) == ["SST-VAL810"]
    assert diagnostics[0].context["path"] == "reference/steps.md"


def test_scripts_are_checked_for_absolute_paths_credentials_and_moved_siblings() -> None:
    authored = skill(
        files={
            "reference/steps.md": "Save the export to ~/notes.txt.\napi_key = 'sk-live-0123456789'\n",
            "reference/data.csv": "a,b\n",
            "scripts/run.py": "open('../reference/data.csv')\nopen('/Users/.../x')\npassword = 'hunter22'\n",
            "load.py": "open('reference/data.csv')\n",
            "blob.bin": b"\xff\xfe",
        }
    )
    _, _, diagnostics = flatten_skill(authored)
    found = [
        (item.code, item.context.get("path"), item.context.get("detail"), item.context.get("target"))
        for item in diagnostics
    ]
    # A path in Markdown is an instruction to the agent; a credential is wrong anywhere.
    assert ("SST-VAL815", "reference/steps.md", "the absolute path '~/notes.txt'", None) not in found
    assert ("SST-VAL815", "reference/steps.md", "a credential literal", None) in found
    assert ("SST-VAL815", "scripts/run.py", "the absolute path '/Users/.../x'", None) in found
    assert ("SST-VAL815", "scripts/run.py", "a credential literal", None) in found
    assert ("SST-VAL833", "scripts/run.py", None, "reference/data.csv") in found
    assert ("SST-VAL833", "load.py", None, "reference/data.csv") in found


def test_catalog_validation_names_layout_uniqueness_and_plugin_membership() -> None:
    good = skill()
    catalog = SkillCatalog(
        (
            good,
            skill("Bad_Name", declared_name="other", body="  ", directory="skills/x/Bad_Name"),
            skill("month-close", directory="skills/y/month-close"),
        ),
        (
            Plugin(
                "month_close", "plugins/mc", "plugins/mc/plugin.yml", "d", None, (), Origin("plugins/mc/plugin.yml")
            ),
            Plugin(
                "finance-kit",
                "plugins/finance-kit",
                "plugins/finance-kit/plugin.yml",
                "d",
                "team",
                ("month-close", "month-close", "ghost"),
                Origin("plugins/finance-kit/plugin.yml"),
            ),
        ),
    )
    diagnostics = validate_skill_catalog(catalog)
    assert codes(diagnostics) == [
        "SST-VAL801",
        "SST-VAL801",
        "SST-RND031",
        "SST-VAL832",
        "SST-VAL837",
        "SST-VAL835",
        "SST-VAL801",
        "SST-VAL832",
        "SST-PRS002",
    ]
    assert validate_skill_catalog(SkillCatalog((good,))) == ()


def test_a_skill_locates_its_own_files_for_a_diagnostic() -> None:
    authored = skill()
    assert authored.origin_of("reference/steps.md", 3, 4) == Origin(
        "skills/finance/month-close/reference/steps.md", 3, 4
    )
    assert authored.origin_of("SKILL.md") == Origin("skills/finance/month-close/SKILL.md")


def test_each_repeated_extension_name_pairs_with_the_first_claim_and_skills_claim_first() -> None:
    first = skill(directory="skills/a/month-close")

    def plugin(name: str) -> Plugin:
        return Plugin(name, f"plugins/{name}", f"plugins/{name}/plugin.yml", "d", None, ("month-close",), Origin("p"))

    # The same record listed twice is two claims: only the second is a repeat.
    catalog = SkillCatalog(
        (skill(directory="skills/c/month-close"), first, skill(directory="skills/b/month-close"), first),
        (plugin("month_close"), plugin("month-close")),
    )
    found = [
        (item.context["artifact"], item.context["other"])
        for item in validate_skill_catalog(catalog)
        if item.code == "SST-VAL832"
    ]
    assert found == [
        ("skills/a/month-close", "skills/a/month-close"),
        ("skills/b/month-close", "skills/a/month-close"),
        ("skills/c/month-close", "skills/a/month-close"),
        ("plugins/month-close", "skills/a/month-close"),
        ("plugins/month_close", "skills/a/month-close"),
    ]


def test_plugin_members_report_each_repeat_once_by_name_then_each_unknown_listing() -> None:
    members = ("zeta", "month-close", "zeta", "alpha", "month-close", "zeta", "alpha")
    plugin = Plugin(
        "kit", "plugins/kit", "plugins/kit/plugin.yml", "d", None, members, Origin("plugins/kit/plugin.yml")
    )
    found = [(item.code, item.context["name"]) for item in validate_skill_catalog(SkillCatalog((skill(),), (plugin,)))]
    assert found == [
        ("SST-VAL837", "alpha"),
        ("SST-VAL837", "month-close"),
        ("SST-VAL837", "zeta"),
        ("SST-VAL835", "zeta"),
        ("SST-VAL835", "zeta"),
        ("SST-VAL835", "alpha"),
        ("SST-VAL835", "zeta"),
        ("SST-VAL835", "alpha"),
    ]


def test_folder_and_plugin_names_follow_one_kebab_case_rule_of_at_most_64_characters() -> None:
    longest, too_long = "a" * 64, "a" * 65
    catalog = SkillCatalog(
        (
            skill(longest, directory=f"skills/{longest}"),
            skill(too_long, directory=f"skills/{too_long}"),
            skill("trailing-", directory="skills/trailing-"),
        ),
        (Plugin("Kit", "plugins/Kit", "plugins/Kit/plugin.yml", "d", None, (longest,), Origin("p")),),
    )
    details = [item.context["detail"] for item in validate_skill_catalog(catalog) if item.code == "SST-VAL801"]
    rule = "is not lowercase kebab-case of at most 64 characters"
    assert details == [
        f"folder name '{too_long}' {rule}",
        f"folder name 'trailing-' {rule}",
        f"plugin name 'Kit' {rule}",
    ]


def test_file_names_a_stage_rejects_fail_validation_at_the_file() -> None:
    unsafe = skill(files={"reference/q1+q2.md": "q", "bad dir/notes.md": "n", "reference/ok.md": "o"})
    diagnostics = validate_skill_catalog(SkillCatalog((unsafe,)))
    assert codes(diagnostics) == ["SST-VAL857", "SST-VAL857"]
    first, second = diagnostics
    assert first.subject == "skill:month-close" and first.origin == Origin(
        "skills/finance/month-close/bad dir/notes.md"
    )
    assert "'bad dir' is not made only of letters" in first.message
    assert second.message.startswith("skill:month-close: reference/q1+q2.md cannot be staged, because 'q1+q2.md'")


def test_skill_bundle_budgets_limits_and_empty_folders() -> None:
    bundle, diagnostics = build_skill_bundle(skill(files={"reference/steps.md": "s"}))
    assert bundle is not None and diagnostics == ()
    assert [entry.path for entry in bundle.entries] == [
        "skills/month-close/SKILL.md",
        "skills/month-close/reference__steps.md",
    ]
    big = "Read reference/steps.md.\n" + "x" * SKILL_MD_BUDGET_BYTES
    over_budget, diagnostics = build_skill_bundle(
        skill(files={"SKILL.md": SKILL_MD.format(name="month-close", body=big), "reference/steps.md": "s"})
    )
    assert over_budget is not None and codes(diagnostics) == ["SST-VAL812"]
    heavy, diagnostics = build_skill_bundle(
        skill(files={"reference/steps.md": "s", "data.bin": b"\0" * (BUNDLE_BUDGET_BYTES + 1)})
    )
    assert heavy is not None and codes(diagnostics) == ["SST-VAL813", "SST-VAL811"]
    many = {f"part{index}.md": "p" for index in range(SCAN_MAX_FILES)}
    too_many, diagnostics = build_skill_bundle(skill(files={"reference/steps.md": "s", **many}))
    assert too_many is None and "SST-VAL834" in codes(diagnostics)
    huge_files = {
        f"blob{index}.bin": b"\0" * (SCAN_MAX_FILE_BYTES + 1)
        for index in range(SCAN_MAX_TOTAL_BYTES // SCAN_MAX_FILE_BYTES)
    }
    huge, diagnostics = build_skill_bundle(skill(files={"reference/steps.md": "s", **huge_files}))
    assert huge is None
    details = [item.context["detail"] for item in diagnostics if item.code == "SST-VAL834"]
    assert any("per-file" in detail for detail in details) and any("in total" in detail for detail in details)
    empty, diagnostics = build_skill_bundle(replace(skill(), files=()))
    assert (empty, diagnostics) == (None, ())


def test_plugin_manifest_and_bundle() -> None:
    members = {
        "month-close": skill(files={"reference/steps.md": "s"}),
        "broken": skill("broken", files={"a/b__c.md": "x", "a__b/c.md": "y"}),
    }
    plugin = Plugin(
        "finance-kit",
        "plugins/finance-kit",
        "plugins/finance-kit/plugin.yml",
        "Finance.",
        "analytics",
        ("month-close", "month-close", "ghost"),
        Origin("plugins/finance-kit/plugin.yml"),
    )
    assert json.loads(plugin_manifest_json(plugin)) == {
        "author": {"name": "analytics"},
        "description": "Finance.",
        "name": "finance-kit",
        "skills": "./skills/",
    }
    bare = Plugin("kit", "plugins/kit", "plugins/kit/plugin.yml", None, None, ("month-close",), Origin("p"))
    assert json.loads(plugin_manifest_json(bare)) == {"name": "kit", "skills": "./skills/"}
    bundle, diagnostics = build_plugin_bundle(plugin, members)
    assert bundle is not None and diagnostics == ()
    assert bundle.kind == "PLUGIN"
    assert [entry.path for entry in bundle.entries] == [
        ".cortex-plugin/plugin.json",
        "skills/month-close/SKILL.md",
        "skills/month-close/reference__steps.md",
    ]
    assert bundle.renames == (("month-close/reference/steps.md", "month-close/reference__steps.md"),)
    blocked, diagnostics = build_plugin_bundle(
        Plugin("kit", "plugins/kit", "plugins/kit/plugin.yml", None, None, ("broken",), Origin("p")), members
    )
    assert blocked is None and codes(diagnostics) == ["SST-VAL836"]
