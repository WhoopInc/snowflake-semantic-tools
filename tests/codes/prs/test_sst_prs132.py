"""SST-PRS132: a skill's front matter, an eval dataset or an eval config writes a key it does not declare."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.evals.config import parse_config
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin, Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.prs_codes import FILE, dataset_findings, skill_tree_findings
from tests.helpers.seam_projects import parsed

CONFIG = "agents/sales/evals/config.yml"
ROW = "  - question: What sold?\n    ground_truth:\n      ground_truth_output: Pizza\n"


def _config_findings(text: str) -> list[Diagnostic]:
    diagnostics: list[Diagnostic] = []
    parse_config((CONFIG, parsed(text, CONFIG)), diagnostics)
    return coded(diagnostics, "SST-PRS132")


def _skill(tmp_path: Path, front: str) -> list[Diagnostic]:
    return skill_tree_findings(tmp_path, "SST-PRS132", {"skills/close/SKILL.md": f"---\n{front}---\nSteps.\n"})


def test_sst_prs132_fires(tmp_path: Path) -> None:
    [skill] = _skill(tmp_path, "name: close\ndescription: Close steps.\nversoin: 2\n")
    assert skill.severity is Severity.WARNING
    assert skill.message == "skill:close: unknown field 'versoin'"
    assert (skill.subject, skill.origin) == ("skill:close", Origin("skills/close/SKILL.md", 1))
    [row] = dataset_findings("SST-PRS132", ROW.replace("    ground_truth:\n", "    weight: 2\n    ground_truth:\n"))
    assert row.message == f"{FILE}: unknown field 'questions[0].weight'"
    assert row.origin == Origin(FILE, 4, 13)
    [near] = _config_findings("name: smoke\nmetrics:\n  system:\n    - name: answer_correctness\n      gaet: true\n")
    assert near.message == f"{CONFIG}: unknown field 'metrics.system[0].gaet'; did you mean 'gate'?"


def test_sst_prs132_silent(tmp_path: Path) -> None:
    # Keys skills write for their agent, and the open skill format's optional keys, are declared.
    assert _skill(tmp_path, "name: close\ndescription: Close steps.\ntools:\n  - Bash\ncategory: ops\n") == []
    # A ground truth keeps keys SST does not read, to send them on; a removed key has its own code.
    assert dataset_findings("SST-PRS132", ROW + "      graded_by: analyst\n") == []
    assert _config_findings("name: smoke\nagent: sales\nmetrics:\n  system:\n    - name: answer_correctness\n") == []
