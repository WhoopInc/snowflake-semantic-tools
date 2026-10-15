"""The published documentation: links resolve, examples parse, and no planning identifier leaks.

The generated reference pages are checked by `sst docs --check`; this file covers the
hand-written pages around them, which nothing else would notice drifting.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

from snowflake_semantic_tools.domain.resolve.ref_fields import REFERENCE_FIELDS

REPO_ROOT = Path(__file__).resolve().parents[2]
PAGES = sorted((REPO_ROOT / "README.md", *(REPO_ROOT / "docs").rglob("*.md")))
FENCE = re.compile(r"^```(\w*)\n(.*?)^```", re.MULTILINE | re.DOTALL)
LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
HEADING = re.compile(r"^#{1,6} (.+)$", re.MULTILINE)
# A decision or rule identifier from the design documents: one letter and three digits.
PLANNING_ID = re.compile(r"(?<![\w$-])[ACDEJKX]\d{3}(?![\w-])")


def _prose(page: Path) -> str:
    return FENCE.sub("", page.read_text(encoding="utf-8"))


def _slug(title: str) -> str:
    return re.sub(r"[^a-z0-9 _-]", "", title.strip().casefold()).replace(" ", "-")


def _anchors(page: Path) -> set[str]:
    return {_slug(title) for title in HEADING.findall(_prose(page))}


@pytest.mark.parametrize("page", PAGES, ids=lambda page: page.relative_to(REPO_ROOT).as_posix())
def test_relative_links_and_anchors_resolve(page: Path) -> None:
    broken = []
    for target in LINK.findall(_prose(page)):
        if re.match(r"[a-z]+:", target):
            continue
        path, _, fragment = target.partition("#")
        destination = (page.parent / path).resolve() if path else page
        anchored = destination.suffix == ".md" and fragment
        if not destination.exists() or (anchored and fragment not in _anchors(destination)):
            broken.append(target)
    assert not broken, f"{page.relative_to(REPO_ROOT)}: {broken}"


@pytest.mark.parametrize("page", PAGES, ids=lambda page: page.relative_to(REPO_ROOT).as_posix())
def test_yaml_and_json_examples_parse(page: Path) -> None:
    for language, body in FENCE.findall(page.read_text(encoding="utf-8")):
        if language == "yaml":
            yaml.safe_load(body)
        elif language == "json":
            json.loads(body)


@pytest.mark.parametrize("page", PAGES, ids=lambda page: page.relative_to(REPO_ROOT).as_posix())
def test_pages_carry_no_planning_identifiers(page: Path) -> None:
    hits = [line.strip() for line in page.read_text(encoding="utf-8").splitlines() if PLANNING_ID.search(line)]
    assert not hits, f"{page.relative_to(REPO_ROOT)}: {hits}"


def test_planning_identifier_rule_bites() -> None:
    assert PLANNING_ID.search("settled by D248 last week")
    assert PLANNING_ID.search("(K202)")
    assert not PLANNING_ID.search("SST-VAL856 and SST-V090")
    assert not PLANNING_ID.search("version: VERSION$3 or SST_16D8F6686433")


# The diagram types GitHub's Mermaid renders. A fence opening with anything else, such as a
# misspelled `stateDiagram-1.0`, renders as an error box, which no other check would notice.
MERMAID_TYPES = frozenset(
    (
        "flowchart",
        "graph",
        "sequenceDiagram",
        "classDiagram",
        "stateDiagram-v2",
        "erDiagram",
        "gantt",
        "pie",
        "journey",
        "gitGraph",
        "mindmap",
        "timeline",
    )
)


def _mermaid_type(body: str) -> str:
    """The first word of a diagram, after any `%%` comment or directive lines."""
    lines = [line.strip() for line in body.splitlines() if line.strip() and not line.strip().startswith("%%")]
    return lines[0].split()[0] if lines else ""


@pytest.mark.parametrize("page", PAGES, ids=lambda page: page.relative_to(REPO_ROOT).as_posix())
def test_mermaid_diagrams_open_with_a_type_github_renders(page: Path) -> None:
    unknown = [
        _mermaid_type(body)
        for language, body in FENCE.findall(page.read_text(encoding="utf-8"))
        if language == "mermaid" and _mermaid_type(body) not in MERMAID_TYPES
    ]
    assert not unknown, f"{page.relative_to(REPO_ROOT)}: {unknown}"


def test_the_mermaid_type_rule_bites() -> None:
    assert _mermaid_type("%% the rings\nflowchart TD\n  a --> b\n") == "flowchart"
    assert _mermaid_type("stateDiagram-1.0\n  [*] --> A\n") not in MERMAID_TYPES
    assert _mermaid_type("\n") == ""


# Files published as content: their bytes are hashed into a bundle, so a golden
# holds them as they are.
PUBLISHED_CONTENT = ("skills/", "plugins/", "profiles/", "hooks/", "mcp-servers/", "commands/")
# Code, the reference fixture, and the goldens, beyond the published pages.
SOURCES = tuple(
    path
    for root in ("snowflake_semantic_tools", "tests/fixtures/reference_project", "tests/golden")
    for path in sorted((REPO_ROOT / root).rglob("*"))
    if path.is_file()
    and path.suffix in {".py", ".yml", ".yaml", ".md", ".sql"}
    and not any(part in {"target", "logs", "dbt_packages", "__pycache__"} for part in path.parts)
    and not any(f"reference_project/{prefix}" in path.as_posix() for prefix in PUBLISHED_CONTENT)
)
# A planning identifier from any series of the design documents.
ANY_PLANNING_ID = re.compile(r"(?<![\w$-])[A-Z]\d{3}(?![\w-])")


@pytest.mark.parametrize("source", SOURCES, ids=lambda path: path.relative_to(REPO_ROOT).as_posix())
def test_code_fixture_and_goldens_carry_no_planning_identifiers(source: Path) -> None:
    hits = [line.strip() for line in source.read_text(encoding="utf-8").splitlines() if ANY_PLANNING_ID.search(line)]
    assert not hits, f"{source.relative_to(REPO_ROOT)}: {hits}"


def test_the_references_guide_lists_every_reference_field() -> None:
    page = (REPO_ROOT / "docs/guides/references.md").read_text(encoding="utf-8")
    missing = [field for field in REFERENCE_FIELDS if f"`{field}`" not in page]
    assert missing == [], f"docs/guides/references.md does not list {missing}"
