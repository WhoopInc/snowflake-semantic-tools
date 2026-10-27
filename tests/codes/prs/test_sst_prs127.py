"""SST-PRS127: a view variable's default is not a value of its data type."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.render.semantic_view import render_variable
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import VIEWS, SmallProject, view_file

VARIABLE = "    variables:\n      - name: rate\n        data_type: {data_type}\n        default_value: {value}\n"


def _reported(tmp_path: Path, data_type: str, value: str) -> list[Diagnostic]:
    files = view_file(VARIABLE.format(data_type=data_type, value=value))
    return list(coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS127"))


@pytest.mark.parametrize(
    "data_type",
    ["NUMBER", "NUMBER(10, 2)", "INT", "INTEGER", "DECIMAL", "DEC", "NUMERIC", "FLOAT", "DOUBLE", "REAL"],
)
def test_sst_prs127_fires(tmp_path: Path, data_type: str) -> None:
    files = view_file(VARIABLE.format(data_type=data_type, value="abc"))
    [diagnostic] = coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS127")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        f"semantic_view:catalog: variable 'rate' of type {data_type} cannot default to 'abc': it takes a finite number"
    )
    assert diagnostic.subject == "semantic_view:catalog"
    # Located at the variable's entry in the view's file.
    assert diagnostic.origin is not None
    assert (diagnostic.origin.file, diagnostic.origin.line) == (VIEWS, 7)


@pytest.mark.parametrize(
    ("data_type", "value"),
    [
        ("DATE", "'2026-13-01'"),
        ("DATE", "soon"),
        ("DATE", "2026-01-01 10:00:00"),
        ("TIMESTAMP_NTZ", "'yesterday'"),
        ("TIMESTAMP_LTZ", "'2026-01-01T25:00:00'"),
        ("TIME", "'noon'"),
        ("BOOLEAN", "3"),
        ("VARCHAR", "3"),
        ("NUMBER", "true"),
        ("VARIANT", "'x'"),
    ],
)
def test_sst_prs127_fires_for_a_default_its_type_cannot_hold(tmp_path: Path, data_type: str, value: str) -> None:
    [diagnostic] = _reported(tmp_path, data_type, value)
    assert f"variable 'rate' of type {data_type} cannot default to" in diagnostic.message


@pytest.mark.parametrize(
    ("data_type", "value"),
    [
        ("INT", "3"),
        ("FLOAT", "1.5"),
        ("BOOLEAN", "false"),
        ("TEXT", "abc"),
        ("DATE", "2026-01-01"),
        ("DATE", "'2026-01-01'"),
        ("TIMESTAMP_NTZ", "2026-01-01 10:00:00"),
        ("TIMESTAMP_TZ", "'2026-01-01T10:00:00+02:00'"),
        ("TIME", "'13:30:00'"),
    ],
)
def test_sst_prs127_silent(tmp_path: Path, data_type: str, value: str) -> None:
    assert _reported(tmp_path, data_type, value) == []


@pytest.mark.parametrize(
    ("data_type", "value", "rendered"),
    [
        ("DATE", "2026-01-01", "RATE DATE DEFAULT '2026-01-01'::DATE"),
        ("DATE", "'2026-01-01'", "RATE DATE DEFAULT '2026-01-01'::DATE"),
        ("TIMESTAMP_NTZ", "2026-01-01 10:00:00", "RATE TIMESTAMP_NTZ DEFAULT '2026-01-01 10:00:00'::TIMESTAMP_NTZ"),
        ("INTEGER", "7", "RATE INTEGER DEFAULT 7"),
    ],
    ids=["yaml-date", "quoted-date", "timestamp", "integer"],
)
def test_a_temporal_default_renders_cast_to_its_type(tmp_path: Path, data_type: str, value: str, rendered: str) -> None:
    files = view_file(VARIABLE.format(data_type=data_type, value=value))
    [view] = SmallProject(tmp_path, files=files).load().views
    [variable] = view.variables
    assert str(render_variable(variable)) == rendered
