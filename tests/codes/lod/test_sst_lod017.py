"""SST-LOD017: a file begins with a UTF-8 byte-order mark."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.cli_projects import validate_json
from tests.helpers.reference_project import project_copy
from tests.helpers.seam_projects import parsed, refused

_BOM = b"\xef\xbb\xbf"
# A procedure tool whose body is a sidecar, added to the reference project.
_TOOL = (
    "tool:\n  name: zz_bom\n  type: procedure\n  description: Returns ok.\n  language: sql\n"
    "  returns: VARCHAR\n  body: \"{{ file('zz_bom.sql') }}\"\n"
)
# Codes a misread mark reported before: the text after it misparsed, or its member dropped out.
_CASCADE = frozenset(("SST-VAL418", "SST-REF011", "SST-PRS034"))


def test_sst_lod017_fires() -> None:
    [diagnostic] = refused(b"\xef\xbb\xbfa: 1\n")
    assert (diagnostic.code, diagnostic.severity) == ("SST-LOD017", Severity.ERROR)
    assert diagnostic.message == "f.yml begins with a BOM"
    assert diagnostic.subject is None
    assert diagnostic.origin == Origin("f.yml")


def test_sst_lod017_silent() -> None:
    assert parsed(b"a: 1\n").tree


def _with_bom(project: Path, relative: str) -> None:
    path = project / relative
    path.write_bytes(_BOM + path.read_bytes())


@pytest.mark.parametrize(
    "sidecar",
    [
        "semantic_models/verified_queries/sql/product_mix_by_type.sql",
        "tools/platform/zz_bom.sql",
        "agents/jaffle_analytics/instructions/response.md",
        "skills/jaffle-catalogue/SKILL.md",
        "coco_profiles/jaffle_analyst/AGENTS.md",
    ],
)
def test_a_bom_in_any_sidecar_is_refused_naming_that_file(tmp_path: Path, sidecar: str) -> None:
    project = project_copy(tmp_path)
    (project / "tools" / "platform" / "zz_bom.yml").write_text(_TOOL, encoding="utf-8")
    (project / "tools" / "platform" / "zz_bom.sql").write_text("BEGIN RETURN 'ok'; END;\n", encoding="utf-8")
    _, clean = validate_json(project)
    assert [item for item in clean if item["code"] == "SST-LOD017"] == []
    _with_bom(project, sidecar)
    code, diagnostics = validate_json(project)
    assert code == 1
    assert [item["location"]["file"] for item in diagnostics if item["code"] == "SST-LOD017"] == [sidecar]
    assert {item["code"] for item in diagnostics} & _CASCADE == set()
