from __future__ import annotations

import dataclasses
import re

import pytest

from snowflake_semantic_tools.domain.diagnostics import ERROR_REFERENCE_URL, ERROR_REGISTRY, ErrorSpec, Severity
from snowflake_semantic_tools.domain.model.config_schema import CONFIG_SCHEMA, ConfigKey, KeyKind, KeyStatus
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY
from snowflake_semantic_tools.domain.render.reference_docs import (
    REFERENCE_DIR,
    CodeCoverage,
    CommandDoc,
    OptionDoc,
    _code,
    _prose,
    _slug,
    _template,
    error_anchor,
    reference_pages,
    render_config,
    render_coverage,
    render_error_codes,
)

COMMANDS = (
    CommandDoc("sst migrate", "Rewrite a project.\n\nMore detail.", subcommands=("sst migrate refs",)),
    CommandDoc(
        "sst migrate refs",
        "Rewrite references.",
        (
            OptionDoc("--write", None, None, "Rewrite in place."),
            OptionDoc("--select", "TEXT", None, "Only `<type>:<name>`.", multiple=True),
            OptionDoc("--suite", "golden|smoke", None, "Which suite.", required=True),
            OptionDoc("--project-dir", "DIRECTORY", ".", "Project root."),
        ),
    ),
)
GLOBAL = (OptionDoc("--version", None, None, "Show the version and exit."),)
EXITS = ((0, "OK", "Success."), (2, "CHANGES", "Changes are pending."))


def test_pages_cover_every_registry_entry() -> None:
    pages = reference_pages(COMMANDS, GLOBAL, EXITS, {})
    assert sorted(pages) == [
        f"{REFERENCE_DIR}/{name}.md" for name in ("artifacts", "cli", "config", "coverage", "error-codes")
    ]
    artifacts = pages[f"{REFERENCE_DIR}/artifacts.md"]
    for artifact in ARTIFACT_REGISTRY.artifacts.values():
        assert f"\n## {artifact.name}\n" in artifacts
        assert artifact.summary and artifact.authored_in and artifact.publishes
    for member in ARTIFACT_REGISTRY.members:
        assert f"| `{member}` |" in artifacts
    config = pages[f"{REFERENCE_DIR}/config.md"]
    for key in CONFIG_SCHEMA:
        assert f"`{key.path}`" in config or f"\n## {key.path}\n" in config, key.path
    cli = pages[f"{REFERENCE_DIR}/cli.md"]
    assert "Subcommands: [`sst migrate refs`](#sst-migrate-refs)." in cli
    assert "| [`sst migrate`](#sst-migrate) | Rewrite a project. |" in cli
    assert "| `--write` | flag |  | Rewrite in place. |" in cli
    assert "| `--select` | TEXT, repeatable |  | Only `<type>:<name>`. |" in cli
    assert "| `--suite` | golden\\|smoke, required |  | Which suite. |" in cli
    assert "| `--project-dir` | DIRECTORY | `.` | Project root. |" in cli
    assert "| 2 | `CHANGES` | Changes are pending. |" in cli


def test_every_help_url_names_a_heading_in_the_error_reference() -> None:
    headings = set(re.findall(r"^### (SST-[A-Z]{3}\d{3})$", render_error_codes(), flags=re.MULTILINE))
    assert headings == set(ERROR_REGISTRY)
    for code, spec in ERROR_REGISTRY.items():
        assert spec.help_url == f"{ERROR_REFERENCE_URL}#{error_anchor(code)}"
        assert _slug(code) == error_anchor(code)


# Retired in 1.0: never raised, or reported by another code. Numbers are not reused.
RETIRED = (
    "SST-CFG045",
    "SST-PRS121",
)


def test_retired_codes_are_absent_from_the_registry_and_the_reference() -> None:
    page = render_error_codes()
    for code in RETIRED:
        assert code not in ERROR_REGISTRY, code
        assert code not in page, code


def test_error_entries_show_severity_placeholders_and_fixes() -> None:
    page = render_error_codes()
    assert "**skill() target not declared** (error)\n\n`{ skill('<name>') } does not resolve`" in page
    assert _template(ERROR_REGISTRY["SST-REF032"]) == "{ skill('<name>') } does not resolve"
    assert "(error, always an error)" in page
    assert "Codes from SST 0.3" not in page and "SST-V090" not in page
    bare = ErrorSpec("SST-CFG999", Severity.INFO, "Bare", "no fix", None, "CFG", "cfg", "url")
    entry = render_error_codes({bare.code: bare}).split("### SST-CFG999", 1)[1]
    assert "Fix:" not in entry and "Raised when" not in entry and "Note:" not in entry
    told = dataclasses.replace(bare, condition="a `<key>` is bare", note="Nothing else.")
    entry = render_error_codes({told.code: told}).split("### SST-CFG999", 1)[1]
    assert "\n\nRaised when a `<key>` is bare.\n\nNote: Nothing else." in entry


def test_every_registered_code_states_when_it_is_raised_without_a_planning_identifier() -> None:
    planning = re.compile(r"(?<![\w$-])[A-Z]\d{1,3}(?![\w-])")
    assert [code for code, spec in ERROR_REGISTRY.items() if not spec.condition] == []
    leaks = [
        code
        for code, spec in ERROR_REGISTRY.items()
        if planning.search(spec.condition or "") or planning.search(spec.note or "") or ".md" in (spec.note or "")
    ]
    assert leaks == []


def test_reference_pages_render_only_what_is_named_and_refuse_an_unknown_reference() -> None:
    pages = reference_pages(COMMANDS, GLOBAL, EXITS, {}, only=("config", "errors", "config"))
    assert sorted(pages) == [f"{REFERENCE_DIR}/config.md", f"{REFERENCE_DIR}/error-codes.md"]
    with pytest.raises(ValueError, match="unknown references: nope"):
        reference_pages(COMMANDS, GLOBAL, EXITS, {}, only=("nope",))


def test_coverage_lists_every_code_and_counts_what_is_missing() -> None:
    spec = ErrorSpec("SST-CFG001", Severity.ERROR, "T", "m", None, "CFG", "cfg", "url")
    other = dataclasses.replace(spec, code="SST-CFG002", severity=Severity.WARNING)
    facts = {"SST-CFG001": CodeCoverage(("a.b", "a.c"), "tests/codes/cfg/test_sst_cfg001.py", "local")}
    page = render_coverage(facts, {spec.code: spec, other.code: other})
    assert "- Codes: 2\n- Raised from no module: 1\n- Without a complete test file: 1\n" in page
    assert (
        "| [`SST-CFG001`](error-codes.md#sst-cfg001) | CFG | error | local | `a.b`<br>`a.c` "
        "| `tests/codes/cfg/test_sst_cfg001.py` |"
    ) in page
    assert "| [`SST-CFG002`](error-codes.md#sst-cfg002) | CFG | warning | - | MISSING | MISSING |" in page


def test_error_codes_refuse_a_subsystem_without_a_section() -> None:
    stray = ErrorSpec("SST-ZZZ001", Severity.ERROR, "Stray", "stray", None, "ZZZ", "zzz", "url")
    with pytest.raises(ValueError, match="ZZZ"):
        render_error_codes({stray.code: stray})


def test_config_page_renders_notes_types_and_removed_keys() -> None:
    schema = (
        ConfigKey("orphan.key", KeyKind.STRING, "A key whose block has no row."),
        ConfigKey("bare", KeyKind.BLOCK, "A block with no keys."),
        ConfigKey("pick", KeyKind.BLOCK, "Pick one.", one_of=("a", "b")),
        ConfigKey("pick.mode", KeyKind.ENUM, "Mode.", default="fast", choices=("fast", "slow"), required=True),
        ConfigKey("pick.count", KeyKind.INTEGER, "How many.", default="the default", minimum=1, maximum=9),
        ConfigKey("pick.gone", KeyKind.ANY, "no longer read", status=KeyStatus.REMOVED),
        ConfigKey("pick.named", KeyKind.ANY, "has its own code", status=KeyStatus.REMOVED, code="SST-CFG040"),
    )
    page = render_config(schema)
    assert "## orphan\n\n| Key |" in page
    assert "## bare\n\nA block with no keys.\n\n## pick" in page
    assert "Declare at least one of `a`, `b`." in page
    assert "| `pick.mode` | enum: `fast`, `slow`, required | `fast` | Mode. |" in page
    assert "| `pick.count` | integer, 1 to 9 | the default | How many. |" in page
    removed = page.split("## Removed keys", 1)[1]
    assert "`pick.gone`" not in page.split("## Removed keys", 1)[0]
    assert "| `pick.gone` | no longer read | [`SST-CFG043`](error-codes.md#sst-cfg043) |" in removed
    assert "| `pick.named` | has its own code | [`SST-CFG040`](error-codes.md#sst-cfg040) |" in removed


def test_fixed_keys_state_their_value_in_their_summary() -> None:
    for key in CONFIG_SCHEMA:
        if key.fixed is not None:
            assert key.summary.startswith(f"Must be {str(key.fixed).lower()}"), key.path


def test_prose_and_code_spans_stay_markdown_safe() -> None:
    assert _prose("a <b> `c<d>` e>f") == "a &lt;b&gt; `c<d>` e&gt;f"
    assert _code("plain") == "`plain`"
    assert _code("uses `x` inside") == "``uses `x` inside``"
    assert _code("`edge`") == "`` `edge` ``"
