"""`-o agent`: a short fixed-grammar document, chosen on its own when a coding agent runs `sst`."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.render.agent import MAX_LINES, agent_document
from snowflake_semantic_tools.cli.render.next_steps import default_next_steps
from snowflake_semantic_tools.cli.render.terminal import AGENT_MARKERS
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.diagnostics.baseline import stable_fingerprint
from tests.helpers.reference_project import DBT_MANIFEST, project_copy

# The grammar every line of an agent document follows.
LINE = re.compile(
    r"^(sst [a-z -]+: (PASS|CHANGES|FAIL|USAGE|CONFIG|CONNECTION|INTERRUPTED) exit=\d+( [a-z_]+=\S+)*"
    r"|[EWI] SST-[A-Z]{3}\d{3} .*\| .*"
    r"|  fix: .+"
    r"|  cascade SST-[A-Z]{3}\d{3} .*\| .*"
    r"|[+~\-!=] \S+ \S+ \S+.*"
    r"|(applied|failed|skipped) \S+ \S+.*"
    r"|\.\.\. \d+ more lines left out; .*"
    r"|next: .+)$"
)


def _broken(tmp_path: Path) -> Path:
    project = project_copy(tmp_path)
    metric = project / "semantic_models" / "metrics" / "total_supply_cost.yml"
    metric.write_text(metric.read_text().replace("'supply_cost'", "'supply_costs'"))
    return project


def _validate(project: Path, *flags: str, env: dict[str, str] | None = None) -> tuple[int, str, str]:
    result = CliRunner().invoke(
        cli, [*flags, "--project-dir", str(project), "--manifest", str(DBT_MANIFEST), "validate"], env=env
    )
    return result.exit_code, result.stdout, result.stderr


def test_a_failed_validate_reads_as_a_verdict_findings_and_a_next_step(tmp_path: Path) -> None:
    code, out, err = _validate(_broken(tmp_path), "-o", "agent")
    lines = out.splitlines()
    assert code == 1 and err == ""
    assert lines[0].startswith("sst validate: FAIL exit=1 errors=1 ")
    assert any(line.startswith("E SST-REF002 semantic_models/metrics/total_supply_cost.yml:") for line in lines)
    assert lines[-1].startswith("next: fix the error, then run `sst validate` again")
    assert all(LINE.match(line) for line in lines), [line for line in lines if not LINE.match(line)]
    assert len(out.encode()) < 2048


def test_a_clean_validate_is_one_verdict_and_a_next_step(tmp_path: Path) -> None:
    code, out, _ = _validate(project_copy(tmp_path), "-o", "agent")
    lines = out.splitlines()
    assert code == 0 and lines[0].startswith("sst validate: PASS exit=0 errors=0 warnings=")
    assert all(LINE.match(line) for line in lines)


@pytest.mark.parametrize("marker", AGENT_MARKERS)
def test_an_agent_marker_chooses_agent_output_and_says_so(tmp_path: Path, marker: str) -> None:
    _, out, _ = _validate(_broken(tmp_path), env={marker: "1"})
    assert out.startswith("sst validate: FAIL ") and f"output=agent(detected {marker})" in out.splitlines()[0]


@pytest.mark.parametrize(
    "flags, env", [(["-o", "table"], {"COCO_AGENT": "1"}), ([], {"COCO_AGENT": "1", "SST_OUTPUT": "table"})]
)
def test_an_asked_for_format_wins_over_detection(tmp_path: Path, flags: list[str], env: dict[str, str]) -> None:
    _, out, err = _validate(_broken(tmp_path), *flags, env=env)
    assert not out.startswith("sst validate:") and "error[SST-REF002]" in err


def test_a_marker_set_to_zero_is_unset(tmp_path: Path) -> None:
    _, _, err = _validate(_broken(tmp_path), env={"COCO_AGENT": "0"})
    assert "error[SST-REF002]" in err


def test_a_refused_command_line_is_an_agent_document(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, ["-o", "agent", "validate", "--bogus"])
    assert result.exit_code == 3
    assert result.stdout.startswith("sst validate: USAGE exit=3 errors=1 ")
    assert result.stdout.splitlines()[-1].startswith("next: see `sst validate --help`")


def test_a_setup_failure_is_an_agent_document(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, ["-o", "agent", "--project-dir", str(tmp_path), "validate"])
    assert result.exit_code == 4 and result.stdout.startswith("sst validate: CONFIG exit=4 ")


def test_a_long_document_is_cut_and_says_where_the_rest_is() -> None:
    bag = DiagnosticBag(tuple(D("SST-REF002", column=f"c{index}", model="m") for index in range(300)))
    out = agent_document(
        "validate", 1, bag, baselined=frozenset(), fingerprint=stable_fingerprint, project_dir=Path(".")
    )
    lines = out.splitlines()
    assert len(lines) == MAX_LINES
    assert lines[-2].startswith("... ") and "`sst -o json validate` has every one" in lines[-2]


def test_baselined_info_and_cascades_are_counted_not_listed() -> None:
    error = D("SST-REF002", subject="metric:m", column="c", model="m", origin=Origin("m.yml", 1, 1))
    held = D("SST-REF002", subject="metric:n", column="d", model="m")
    cascade = D("SST-MEM107", subject="metric:m", member="metric:m", count=1, caused_by="SST-REF002")
    stray = D("SST-MEM107", subject="metric:x", member="metric:x", count=1, caused_by="SST-VAL999")
    bag = DiagnosticBag((error, held, cascade, stray))
    out = agent_document(
        "validate",
        1,
        bag,
        baselined=frozenset({stable_fingerprint(held)}),
        fingerprint=stable_fingerprint,
        project_dir=Path("."),
    )
    first, *rest = out.splitlines()
    assert "baselined=1" in first and "cascade_hidden=1" in first
    assert sum(line.startswith("E ") for line in rest) == 1
    assert any(line.startswith("  cascade SST-MEM107 ") for line in rest)


def test_a_plan_lists_its_changes_and_counts_the_unchanged() -> None:
    data = {
        "counts": {"create": 1, "update": 1, "prune": 0, "blocked": 0, "noop": 3},
        "changes": [
            {"action": "create", "artifact_key": "semantic_view:a", "target": "DB.S.A", "reason": "not_present"},
            {
                "action": "update",
                "artifact_key": "agent:b",
                "target": "DB.S.B",
                "reason": "fingerprint_differs",
                "properties": [{"property": "fingerprint"}, {"property": "manifest_id"}, {"property": "aliases"}],
            },
            {"action": "noop", "artifact_key": "semantic_view:c", "target": "DB.S.C", "reason": "unchanged"},
            # Report-only, with nothing observed: no target, as the table shows it.
            {"action": "prune", "artifact_key": "tool:d", "target": None, "reason": "orphaned", "report_only": True},
        ],
    }
    out = agent_document(
        "plan",
        2,
        DiagnosticBag(),
        baselined=frozenset(),
        fingerprint=stable_fingerprint,
        project_dir=Path("."),
        data=data,
        next_steps=default_next_steps("plan", 2, errors=0, warnings=0, audience="agent"),
    )
    lines = out.splitlines()
    assert lines[0].startswith(
        "sst plan: CHANGES exit=2 errors=0 warnings=0 create=1 update=1 prune=0 blocked=0 unchanged=3"
    )
    assert lines[1:4] == [
        "+ semantic_view:a DB.S.A not_present",
        "~ agent:b DB.S.B fingerprint_differs changes=definition,version_alias",
        "- tool:d - orphaned report_only",
    ]
    assert "run it only when they ask" in lines[-1]
    assert all(LINE.match(line) for line in lines)


def test_apply_lists_what_it_did() -> None:
    data = {"outcomes": [{"action": "update", "artifact_key": "agent:b", "status": "failed", "error": "no\nprivilege"}]}
    out = agent_document(
        "apply",
        1,
        DiagnosticBag(),
        baselined=frozenset(),
        fingerprint=stable_fingerprint,
        project_dir=Path("."),
        data=data,
    )
    assert "failed agent:b update | no privilege" in out.splitlines()


@pytest.mark.parametrize("exit_code", [0, 1, 2, 3, 4, 5, 130])
@pytest.mark.parametrize("command", ["validate", "plan", "apply", "drop", "baseline add", "test", "eval"])
def test_next_never_tells_an_agent_to_change_snowflake_on_its_own(command: str, exit_code: int) -> None:
    for step in default_next_steps(command, exit_code, errors=1, warnings=1, audience="agent"):
        assert not re.search(r"\brun `sst (apply|drop|baseline add|eval)\b", step), step


def test_project_text_keeps_its_characters_and_loses_its_control_codes() -> None:
    bag = DiagnosticBag((D("SST-REF002", column="caf\u00e9\x1b[31m", model="m"),))
    out = agent_document(
        "validate", 1, bag, baselined=frozenset(), fingerprint=stable_fingerprint, project_dir=Path(".")
    )
    assert "caf\u00e9" in out and "\x1b" not in out


def test_json_lists_the_next_steps(tmp_path: Path) -> None:
    _, out, _ = _validate(_broken(tmp_path), "-o", "json")
    steps = json.loads(out)["next_steps"]
    assert steps[0].startswith("fix the error") and any("sst explain" in step for step in steps)
