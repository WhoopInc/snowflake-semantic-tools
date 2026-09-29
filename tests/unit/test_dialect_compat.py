"""The 0.3 authoring dialect: rejected as authored, converted by `sst migrate refs`.

The corpus is synthetic, so the guarantee is asserted in public CI without any
production content. Assertion numbers follow the compatibility suite contract:
converted files parse with no errors (1) and render the committed DDL (2); the
legacy globals are rejected with their specific codes and render nothing (3, 6);
the codemod is idempotent (4); an indented root key still yields its
relationships (5); and the deprecated relationship shape parses with a warning (7).
"""

from __future__ import annotations

import json
import shutil
from collections import Counter
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli

ROOT = Path(__file__).parents[2]
CORPUS = ROOT / "tests" / "fixtures" / "v1_dialect"
MANIFEST = ROOT / "tests" / "fixtures" / "reference_project_manifest.json"


def invoke(*args: str) -> tuple[int, dict[str, object]]:
    result = CliRunner().invoke(cli, [*args, "--output", "json"])
    return result.exit_code, json.loads(result.output)


def codes(payload: dict[str, object]) -> Counter[tuple[str, str]]:
    diagnostics = payload["diagnostics"]
    assert isinstance(diagnostics, list)
    return Counter((item["code"], item["severity"]) for item in diagnostics)


def converted_copy(tmp_path: Path) -> Path:
    project = tmp_path / "project"
    shutil.copytree(CORPUS, project, ignore=shutil.ignore_patterns("expected", "target"))
    return project


def test_legacy_globals_are_rejected_with_their_codes_and_render_nothing() -> None:
    exit_code, payload = invoke("compile", "--project-dir", str(CORPUS), "--manifest", str(MANIFEST))
    assert exit_code == 1
    assert codes(payload) == Counter(
        {
            ("SST-REF034", "error"): 10,
            ("SST-REF035", "error"): 7,
            ("SST-VAL405", "error"): 1,
            ("SST-PRS020", "warning"): 3,
        }
    )


def test_codemod_converts_idempotently_and_the_result_renders_the_committed_ddl(tmp_path: Path) -> None:
    project = converted_copy(tmp_path)
    pending, report = invoke("migrate", "refs", "--project-dir", str(project))
    assert pending == 2
    files = report["data"]["files"]  # type: ignore[index]
    assert {item["path"]: item["rewrites"] for item in files} == {
        "semantic_models/filters/filters.yml": {"ref": 1, "bare": 0, "column": 1, "labels": 1},
        "semantic_models/metrics/metrics.yml": {"ref": 2, "bare": 0, "column": 2, "labels": 0},
        "semantic_models/relationships/relationships.yml": {"ref": 0, "bare": 4, "column": 4, "labels": 0},
        "semantic_models/semantic_views/views.yml": {"ref": 3, "bare": 0, "column": 0, "labels": 0},
    }
    written, _ = invoke("migrate", "refs", "--project-dir", str(project), "--write")
    assert written == 0
    expected = CORPUS / "expected" / "converted"
    for path in sorted(expected.rglob("*.yml")):
        relative = path.relative_to(expected)
        assert (project / relative).read_bytes() == path.read_bytes(), relative
    again, report = invoke("migrate", "refs", "--project-dir", str(project))
    assert again == 0 and report["data"]["files"] == []  # type: ignore[index]

    exit_code, payload = invoke("compile", "--project-dir", str(project), "--manifest", str(MANIFEST))
    assert exit_code == 0, payload
    assert codes(payload) == Counter({("SST-PRS020", "warning"): 3})
    fields = {item["params"]["field"] for item in payload["diagnostics"]}  # type: ignore[index]
    assert fields == {"sql_generation", "question_categorization", "relationship_columns"}
    rendered = tmp_path / "ddl"
    CliRunner().invoke(
        cli, ["compile", "--project-dir", str(project), "--manifest", str(MANIFEST), "--emit-ddl", str(rendered)]
    )
    ddl = (rendered / "dialect_sales.sql").read_text(encoding="utf-8")
    assert ddl == (CORPUS / "expected" / "ddl" / "dialect_sales.sql").read_text(encoding="utf-8")
    assert "ORDERS_TO_CUSTOMERS AS ORDERS (CUSTOMER_ID) REFERENCES CUSTOMERS (CUSTOMER_ID)" in ddl
    assert "ORDERS_TO_LOCATIONS AS ORDERS (LOCATION_ID) REFERENCES LOCATIONS (LOCATION_ID)" in ddl
    assert "ORDERS.DIALECT_COMPLETED_ORDER LABELS = (FILTER)" in ddl
    assert "AI_SQL_GENERATION 'Monetary columns" in ddl and "AI_QUESTION_CATEGORIZATION 'Decline" in ddl


def test_both_spellings_on_one_entry_are_an_error(tmp_path: Path) -> None:
    project = converted_copy(tmp_path)
    CliRunner().invoke(cli, ["migrate", "refs", "--project-dir", str(project), "--write"])
    instructions = project / "semantic_models" / "custom_instructions" / "custom_instructions.yml"
    instructions.write_text(
        instructions.read_text(encoding="utf-8").replace(
            "    sql_generation: |-", "    ai_sql_generation: Newer text.\n    sql_generation: |-"
        ),
        encoding="utf-8",
    )
    exit_code, payload = invoke("compile", "--project-dir", str(project), "--manifest", str(MANIFEST))
    assert exit_code == 1
    assert codes(payload)[("SST-PRS121", "error")] == 1
