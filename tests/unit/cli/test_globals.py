"""The global options: placement, environment variables, reserved short options, and the baseline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click
import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common, project_copy


def _json(args: list[str], env: dict[str, str] | None = None) -> tuple[int, dict[str, Any]]:
    result = CliRunner().invoke(cli, args, env=env)
    return result.exit_code, json.loads(result.output)


def test_a_global_option_after_the_command_wins_over_one_before_it(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    result = CliRunner().invoke(cli, ["-o", "table", "validate", *common(project), "-o", "json"])
    assert json.loads(result.output)["command"] == "validate"
    before = CliRunner().invoke(cli, ["--project-dir", str(project), "--manifest", str(common(project)[3]), "validate"])
    assert before.exit_code == 0 and "validated 14 artifact(s)" in before.output


def _leaf_commands(group: click.Group, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], click.Command]]:
    found: list[tuple[tuple[str, ...], click.Command]] = []
    for name, command in sorted(group.commands.items()):
        if isinstance(command, click.Group):
            found.extend(_leaf_commands(command, (*path, name)))
        else:
            found.append(((*path, name), command))
    return found


def test_every_command_lists_the_global_options_in_its_own_help() -> None:
    # Consumers check what a command accepts by reading `sst <command> --help`.
    for path, command in _leaf_commands(cli):
        assert isinstance(command, SstCommand), path
        shown = CliRunner().invoke(cli, [*path, "--help"])
        assert shown.exit_code == 0, path
        own, _, global_section = shown.output.partition("\nGlobal options:\n")
        own = own.partition("\nOptions:\n")[2]
        assert "--output" in global_section and "--manifest" in global_section, path
        assert "  -o, --output" not in own and "--help" in own, path


def test_environment_variables_supply_global_and_command_options(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    env = {"SST_OUTPUT": "json", "SST_PROJECT_DIR": str(project), "SST_TARGET": "prod", "SST_NO_COLOR": "YES"}
    exit_code, envelope = _json(["validate", "--manifest", common(project)[3]], env)
    assert exit_code == 0 and envelope["invocation"]["target"] == "prod"
    assert envelope["invocation"]["project_dir"] == str(project.resolve())
    # The command line outranks the environment.
    assert _json(["validate", *common(project), "-t", "dev"], env)[1]["invocation"]["target"] == "dev"
    explicit = tmp_path / "ci.yml"
    explicit.write_text((project / "sst_config.yml").read_text(encoding="utf-8"), encoding="utf-8")
    exit_code, envelope = _json(["validate", *common(project)], {**env, "SST_CONFIG": str(explicit)})
    assert envelope["invocation"]["config_file"] == str(explicit.resolve())
    assert "SST-CFG032" in [item["code"] for item in envelope["diagnostics"]]


@pytest.mark.parametrize("name", ["SST_STRICT", "SST_NO_COLOR"])
def test_an_unparseable_boolean_variable_is_a_configuration_error(tmp_path: Path, name: str) -> None:
    project = project_copy(tmp_path)
    result = CliRunner().invoke(cli, ["validate", *common(project)], env={name: "maybe"})
    assert result.exit_code == 4 and "error[SST-CFG004]" in result.output
    exit_code, envelope = _json(["validate", *common(project), "--output", "json"], {name: "maybe"})
    [diagnostic] = envelope["diagnostics"]
    assert (exit_code, diagnostic["code"]) == (4, "SST-CFG004")
    assert diagnostic["message"] == f"config key '{name}' expects boolean, found 'maybe'"


def test_an_unknown_environment_choice_is_a_configuration_error_too(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, ["validate", *common(project_copy(tmp_path))], env={"SST_LOG_LEVEL": "loud"})
    assert result.exit_code == 4 and "SST-CFG004" in result.output


@pytest.mark.parametrize(
    ("letter", "names"),
    [("-d", "--database"), ("-f", "--output"), ("-a", "--select"), ("-m", "--select"), ("-s", "--select")],
)
def test_each_reserved_short_option_is_refused_naming_its_replacement(tmp_path: Path, letter: str, names: str) -> None:
    result = CliRunner().invoke(cli, ["validate", *common(project_copy(tmp_path)), letter])
    assert result.exit_code == 3 and "SST-PRT100" in result.output and names in result.output
    assert CliRunner().invoke(cli, [letter, "validate"]).exit_code == 3


def _warnings(project: Path, *flags: str) -> tuple[int, dict[str, Any]]:
    return _json(["validate", *common(project), "--no-strict", *flags, "--output", "json"])


def _baseline(project: Path, fingerprints: list[str], expires_on: str = "2999-01-01") -> None:
    (project / ".sst").mkdir(exist_ok=True)
    entries = [{"fingerprint": value, "code": "SST-CFG018"} for value in fingerprints]
    (project / ".sst" / "baseline.json").write_text(
        json.dumps({"version": 1, "expires_on": expires_on, "entries": entries}), encoding="utf-8"
    )


def test_a_baseline_marks_and_counts_what_it_holds_and_never_drops_it(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    _, before = _warnings(project)
    held = [item["fingerprint"] for item in before["diagnostics"] if item["code"] == "SST-CFG018"]
    assert len(held) == 2 and all(len(value) == 16 for value in held)
    _baseline(project, held)
    _, after = _warnings(project)
    assert after["summary"]["baselined"] == 2 and len(after["diagnostics"]) == len(before["diagnostics"])
    assert [item["baselined"] for item in after["diagnostics"] if item["code"] == "SST-CFG018"] == [True] * 2
    assert _warnings(project, "--no-baseline")[1]["summary"]["baselined"] == 0
    assert "no-baseline" in _warnings(project, "--no-baseline")[1]["invocation"]["overrides"][0]
    human = CliRunner().invoke(cli, ["validate", *common(project), "--no-strict"])
    assert (
        "SST-CFG018" not in human.output and "(2 baselined)" in human.output and "2 baselined not shown" in human.output
    )
    shown = CliRunner().invoke(cli, ["validate", *common(project), "--no-strict", "--show-baselined", "--show-info"])
    assert shown.output.count("warning[SST-CFG018]") == 2 and "info[SST-VAL854]" in shown.output


def test_strict_does_not_block_on_what_the_baseline_holds(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    _, before = _warnings(project)
    _baseline(project, [item["fingerprint"] for item in before["diagnostics"] if item["code"] == "SST-CFG018"])
    strict = _json(["validate", *common(project), "--strict", "--output", "json"])
    # SST-VAL528 is not baselined, so the promoted warning still blocks.
    assert strict[0] == 1 and strict[1]["summary"]["promoted"] >= 1
    all_held = [item["fingerprint"] for item in before["diagnostics"] if item["severity"] == "warning"]
    _baseline(project, all_held)
    assert _json(["validate", *common(project), "--strict", "--output", "json"])[0] == 0


def test_an_expired_or_unreadable_baseline_fails_the_run(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    _baseline(project, ["0" * 16], expires_on="2000-01-01")
    exit_code, envelope = _warnings(project)
    assert exit_code == 1 and envelope["diagnostics"][0]["code"] == "SST-CFG039"
    (project / ".sst" / "baseline.json").write_text("{not json", encoding="utf-8")
    exit_code, envelope = _warnings(project)
    assert (exit_code, envelope["diagnostics"][0]["code"]) == (4, "SST-PRT009")
    for document in ('{"version": 2}', '{"version": 1}', '{"version": 1, "expires_on": "x", "entries": [1]}'):
        (project / ".sst" / "baseline.json").write_text(document, encoding="utf-8")
        assert _warnings(project)[0] == 4
    missing = _warnings(project, "--baseline", str(tmp_path / "missing.json"))
    assert (missing[0], missing[1]["diagnostics"][0]["code"]) == (4, "SST-PRT009")


def test_severity_overrides_change_what_a_code_reports_and_what_blocks(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    config = project / "sst_config.yml"
    config.write_text(
        config.read_text(encoding="utf-8") + "\ndiagnostics:\n  severity_overrides:\n    SST-CFG018: error\n",
        encoding="utf-8",
    )
    exit_code, envelope = _warnings(project)
    promoted = [item for item in envelope["diagnostics"] if item["code"] == "SST-CFG018"]
    assert exit_code == 1 and {(item["severity"], item["promoted_from"]) for item in promoted} == {("error", "warning")}


def test_verbose_quiet_grouping_and_colour_shape_only_the_human_render(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    args = ["validate", *common(project), "--no-strict"]
    verbose = CliRunner().invoke(cli, [*args, "-v"])
    assert "phase: cfg; fingerprint: " in verbose.output
    quiet = CliRunner().invoke(cli, [*args, "-q"])
    assert "warning[" not in quiet.output and "errors;" not in quiet.output
    plain = CliRunner().invoke(cli, [*args, "-o", "plain"])
    assert "\x1b[" not in plain.output and "warning[SST-CFG018]" in plain.output


def test_four_or_more_of_one_code_collapse_to_three_and_a_count(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    partner = project / "tools" / "jaffle_partner.yml"
    extra = "\n".join(
        f"      - name: extra_{index}\n        type: generic\n        description: Extra.\n"
        f"        relations:\n          dev: A.B.C{index}\n          prod: D.E.F{index}"
        for index in range(3)
    )
    partner.write_text(partner.read_text(encoding="utf-8").rstrip() + "\n" + extra + "\n", encoding="utf-8")
    grouped = CliRunner().invoke(cli, ["validate", *common(project), "--no-strict"])
    assert grouped.output.count("warning[SST-CFG018]") == 3
    assert "... and 2 more SST-CFG018 (--show-all-occurrences)" in grouped.output
    every = CliRunner().invoke(cli, ["validate", *common(project), "--no-strict", "--show-all-occurrences"])
    assert every.output.count("warning[SST-CFG018]") == 5
