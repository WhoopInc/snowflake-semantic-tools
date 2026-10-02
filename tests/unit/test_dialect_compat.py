"""The 0.3 authoring dialect: rejected as authored, converted by `sst migrate refs`.

The corpus is synthetic, so the guarantee is asserted in public CI without any
production content. Assertion numbers follow the compatibility suite contract:
converted files parse with no errors (1) and render the committed DDL (2); the
legacy globals are rejected with their specific codes and render nothing (3, 6);
the codemod is idempotent (4); an indented root key still yields its
relationships (5); and the 0.3 key spellings, which the codemod leaves alone, are
errors naming the 1.0 keys (7).
"""

from __future__ import annotations

import json
import shutil
from collections import Counter
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.projects import SEAM_NOTES

ROOT = Path(__file__).parents[2]
CORPUS = ROOT / "tests" / "fixtures" / "v1_dialect"
MANIFEST = ROOT / "tests" / "fixtures" / "reference_project_manifest.json"


def invoke(*args: str) -> tuple[int, dict[str, Any]]:
    result = CliRunner().invoke(cli, [*args, "--output", "json"])
    return result.exit_code, json.loads(result.output)


def codes(payload: dict[str, object]) -> Counter[tuple[str, str]]:
    """Count the payload's diagnostics by code and severity, leaving out the dbt seam's notes."""
    diagnostics = payload["diagnostics"]
    assert isinstance(diagnostics, list)
    return Counter((item["code"], item["severity"]) for item in diagnostics if item["code"] not in SEAM_NOTES)


def converted_copy(tmp_path: Path) -> Path:
    project = tmp_path / "project"
    shutil.copytree(CORPUS, project, ignore=shutil.ignore_patterns("expected", "target"))
    return project


def rename_0_3_keys(project: Path) -> None:
    """The hand edit SST-PRS020 asks for after the codemod: the 1.0 key names."""
    models = project / "semantic_models"
    instructions = models / "custom_instructions" / "custom_instructions.yml"
    text = instructions.read_text(encoding="utf-8")
    text = text.replace("    sql_generation:", "    ai_sql_generation:")
    instructions.write_text(text.replace("    question_categorization:", "    ai_question_categorization:"), "utf-8")
    relationships = models / "relationships" / "relationships.yml"
    relationships.write_text(
        relationships.read_text(encoding="utf-8").replace(
            "    relationship_columns:\n"
            "      - left_column: \"{{ ref('orders', 'customer_id') }}\"\n"
            "        right_column: \"{{ ref('customers', 'customer_id') }}\"\n",
            "    relationship_conditions:\n"
            "      - \"{{ ref('orders', 'customer_id') }} = {{ ref('customers', 'customer_id') }}\"\n",
        ),
        encoding="utf-8",
    )


def test_legacy_globals_are_rejected_with_their_codes_and_render_nothing() -> None:
    exit_code, payload = invoke("compile", "--project-dir", str(CORPUS), "--manifest", str(MANIFEST))
    assert exit_code == 1
    assert codes(payload) == Counter(
        {
            ("SST-MAN008", "error"): 1,
            ("SST-REF034", "error"): 10,
            ("SST-REF035", "error"): 7,
            ("SST-VAL405", "error"): 1,
            ("SST-PRS020", "warning"): 2,
            ("SST-PRS021", "error"): 1,
            # The manifest's file checksums read each file first; compile is served the cache.
            ("SST-LOD201", "info"): 5,
        }
    )


def test_codemod_converts_idempotently_and_the_result_renders_the_committed_ddl(tmp_path: Path) -> None:
    project = converted_copy(tmp_path)
    pending, report = invoke("migrate", "refs", "--project-dir", str(project))
    assert pending == 2
    files = report["data"]["files"]
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
    assert again == 0 and report["data"]["files"] == []

    # The codemod rewrites references, not keys: the 0.3 spellings remain, each
    # deprecated spelling naming its 1.0 key, and the 0.3 relationship shape an error.
    exit_code, payload = invoke("compile", "--project-dir", str(project), "--manifest", str(MANIFEST))
    assert exit_code == 1
    assert codes(payload) == Counter(
        {
            ("SST-PRS020", "warning"): 2,
            ("SST-PRS021", "error"): 1,
            ("SST-MAN008", "error"): 1,
            ("SST-LOD201", "info"): 5,
        }
    )
    renames = {
        (item["params"]["field"], item["params"]["expected"])
        for item in payload["diagnostics"]
        if item["code"] == "SST-PRS020"
    }
    assert renames == {
        ("sql_generation", "ai_sql_generation"),
        ("question_categorization", "ai_question_categorization"),
    }

    rename_0_3_keys(project)
    exit_code, payload = invoke("compile", "--project-dir", str(project), "--manifest", str(MANIFEST))
    assert exit_code == 0, payload
    assert codes(payload) == Counter({("SST-LOD201", "info"): 5})
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


def test_a_0_3_spelling_beside_its_1_0_key_is_reported_and_not_read(tmp_path: Path) -> None:
    project = converted_copy(tmp_path)
    CliRunner().invoke(cli, ["migrate", "refs", "--project-dir", str(project), "--write"])
    rename_0_3_keys(project)
    instructions = project / "semantic_models" / "custom_instructions" / "custom_instructions.yml"
    instructions.write_text(
        instructions.read_text(encoding="utf-8").replace(
            "    ai_sql_generation: |-", "    sql_generation: Older text.\n    ai_sql_generation: |-"
        ),
        encoding="utf-8",
    )
    exit_code, payload = invoke("compile", "--project-dir", str(project), "--manifest", str(MANIFEST))
    assert exit_code == 0
    assert codes(payload) == Counter({("SST-PRS020", "warning"): 1, ("SST-LOD201", "info"): 5})
