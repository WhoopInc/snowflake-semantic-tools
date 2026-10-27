"""SST-LOD022: text holds a NUL or a lone surrogate, which Snowflake cannot store."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.cli_projects import validate_json
from tests.helpers.reference_project import project_copy
from tests.helpers.seam_projects import parsed, refused

# Each file a hostile value is written into, its text up to the value and after it; the value
# sits on the last line of the text before it.
_VIEW = (
    "semantic_models/semantic_views/zz_hostile.yml",
    b"semantic_view:\n  name: zz_hostile\n  description: ",
    b"\n  tables:\n    - \"{{ ref('products') }}\"\n",
)
_TOOL = (
    "tools/platform/zz_hostile.yml",
    b"tool:\n  name: zz_hostile\n  type: procedure\n  description: ",
    b"\n  language: sql\n  returns: VARCHAR\n  body: \"{{ file('zz_hostile.sql') }}\"\n",
)
_AGENT = ("agents/ops/zz_hostile/agent.yml", b"name: zz_hostile_agent\ncomment: ", b"\n")
# A raw NUL byte, YAML's `\0` escape, and a lone surrogate written as an escape.
_VALUES = (b"a\x00b", b'"a\\0b"', b'"a\\ud800b"')
_INTERNAL = frozenset(("SST-INT001", "SST-INT902"))


def test_sst_lod022_fires() -> None:
    [diagnostic] = refused(b'a: 1\nb:\n  c: "x\\0y"\n')
    assert (diagnostic.code, diagnostic.severity) == ("SST-LOD022", Severity.ERROR)
    assert diagnostic.message == "f.yml: 'b.c' holds a NUL character, which Snowflake cannot store"
    assert diagnostic.origin == Origin("f.yml", 3)


def test_sst_lod022_silent() -> None:
    assert parsed(b'a: "x\\ty \\u00e9"\n').tree == {"a": "x\ty \u00e9"}


def test_a_raw_nul_is_refused_at_its_line_before_yaml_reads_it() -> None:
    [diagnostic] = refused(b"a: 1\nb: x\x00y\n")
    assert diagnostic.message == "f.yml: 'text' holds a NUL character, which Snowflake cannot store"
    assert diagnostic.origin == Origin("f.yml", 2)


def test_a_surrogate_in_a_key_is_refused_at_its_mapping() -> None:
    [diagnostic] = refused(b'a:\n  "k\\ud800": 1\n')
    assert diagnostic.context["found"] == "the lone surrogate U+D800"
    assert diagnostic.origin == Origin("f.yml", 2)


def _validated(tmp_path: Path, relative: str, content: bytes) -> tuple[int, list[dict[str, object]]]:
    """Validate a reference project copy with the procedure tool and its body, then `content` at `relative`."""
    project = project_copy(tmp_path)
    (project / _TOOL[0]).write_bytes(_TOOL[1] + b"Returns ok." + _TOOL[2])
    (project / "tools" / "platform" / "zz_hostile.sql").write_text("BEGIN RETURN 'ok'; END;\n", encoding="utf-8")
    path = project / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return validate_json(project)


@pytest.mark.parametrize("site", [_VIEW, _TOOL, _AGENT], ids=["view", "procedure", "agent"])
@pytest.mark.parametrize("value", _VALUES, ids=["nul", "nul-escape", "surrogate-escape"])
def test_a_hostile_character_in_authored_text_is_refused_naming_its_file_and_line(
    tmp_path: Path, site: tuple[str, bytes, bytes], value: bytes
) -> None:
    relative, before, after = site
    code, diagnostics = _validated(tmp_path, relative, before + value + after)
    assert code == 1
    found = [item for item in diagnostics if item["code"] == "SST-LOD022"]
    assert [(item["location"]["file"], item["location"]["line"]) for item in found] == [  # type: ignore[index]
        (relative, before.count(b"\n") + 1)
    ]
    assert {item["code"] for item in diagnostics} & _INTERNAL == set()


def test_a_nul_in_a_body_file_is_refused_naming_the_body(tmp_path: Path) -> None:
    code, diagnostics = _validated(tmp_path, "tools/platform/zz_hostile.sql", b"BEGIN\n  RETURN 'a\x00b';\nEND;\n")
    assert code == 1
    found = [item for item in diagnostics if item["code"] == "SST-LOD022"]
    assert [(item["location"]["file"], item["location"]["line"]) for item in found] == [  # type: ignore[index]
        ("tools/platform/zz_hostile.sql", 2)
    ]
    assert {item["code"] for item in diagnostics} & _INTERNAL == set()
