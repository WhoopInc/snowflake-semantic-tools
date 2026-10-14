"""No authored YAML -- or a file it names -- is read through a symbolic link or from outside the project.

Discovery of semantic models, and the agent, tool, and eval-metric loaders, refuse a linked
file or folder with SST-PRT009 wherever it points, inside the project or out; a link that
loops is SST-DIS005. The file a verified query's `sql:` loads with `file()`, and a tool's
`body_file:`, are refused the same way, and may not climb out of the project with `..` or an
absolute path (for a verified query, SST-REF027). The input
checksums a manifest records skip every link the loaders refuse.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.project_source import _file_checksums
from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.adapters.yaml.discover import discover_yaml
from snowflake_semantic_tools.adapters.yaml.documents import load_documents
from snowflake_semantic_tools.adapters.yaml.evals.metrics import load_custom_metrics
from snowflake_semantic_tools.adapters.yaml.fields import OUTSIDE_PROJECT
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.adapters.yaml.semantic.file_reads import _verified_query_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.readers import load_verified_queries
from snowflake_semantic_tools.adapters.yaml.tools import load_tool_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject

LINKED = "it is a symbolic link, which SST does not follow"
METRICS = "snowflake_metrics: []\n"
SMUGGLED_VIEW = (
    "semantic_views:\n"
    "  - name: smuggled\n"
    "    description: Use this view for questions about what should never be published.\n"
    "    tables:\n"
    "      - \"{{ ref('products') }}\"\n"
)


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _refusals(diagnostics: tuple[Diagnostic, ...] | list[Diagnostic]) -> list[tuple[str, str]]:
    return [(item.context["path"], item.context["detail"]) for item in coded(diagnostics, "SST-PRT009")]


# --- semantic-model discovery ---------------------------------------------------------------


def test_a_linked_model_file_pointing_inside_the_project_is_refused(tmp_path: Path) -> None:
    project = tmp_path / "project"
    _write(project / "semantic_models" / "real.yml", METRICS)
    _write(project / "elsewhere" / "inside.yml", METRICS)
    (project / "semantic_models" / "linked.yml").symlink_to(project / "elsewhere" / "inside.yml")
    found = discover_yaml(project, "semantic_models")
    assert [item.path for item in found.files] == ["semantic_models/real.yml"]
    [diagnostic] = found.diagnostics
    assert diagnostic.message == f"could not read semantic_models/linked.yml: {LINKED}"
    assert diagnostic.origin == Origin("semantic_models/linked.yml")


def test_a_linked_model_file_pointing_outside_the_project_is_never_loaded(tmp_path: Path) -> None:
    project = tmp_path / "project"
    _write(project / "semantic_models" / "real.yml", METRICS)
    outside = _write(tmp_path / "outside.yml", SMUGGLED_VIEW)
    (project / "semantic_models" / "evil.yml").symlink_to(outside)
    documents = load_documents(discover_yaml(project, "semantic_models"), parse_yaml_bytes)
    assert [document.path for document in documents.documents] == ["semantic_models/real.yml"]
    assert _refusals(documents.diagnostics) == [("semantic_models/evil.yml", LINKED)]


def test_a_linked_model_outside_the_project_does_not_reach_the_compiled_views(tmp_path: Path) -> None:
    small = SmallProject(tmp_path / "project")
    small.write()
    outside = _write(tmp_path / "outside.yml", SMUGGLED_VIEW)
    (small.root / "semantic_models" / "semantic_views" / "evil.yml").symlink_to(outside)
    loaded = small.load()
    names = [view.fqn.rsplit(".", 1)[-1].casefold() for view in loaded.views]
    assert "catalog" in names and "smuggled" not in names
    assert _refusals(loaded.diagnostics) == [("semantic_models/semantic_views/evil.yml", LINKED)]


@pytest.mark.parametrize("inside", [True, False], ids=["inside", "outside"])
def test_a_linked_directory_is_refused_and_nothing_below_it_is_read(tmp_path: Path, inside: bool) -> None:
    project = tmp_path / "project"
    _write(project / "semantic_models" / "real.yml", METRICS)
    target = (project / "shared") if inside else (tmp_path / "shared")
    _write(target / "hidden.yml", SMUGGLED_VIEW)
    (project / "semantic_models" / "shared").symlink_to(target)
    found = discover_yaml(project, "semantic_models")
    assert [item.path for item in found.files] == ["semantic_models/real.yml"]
    assert _refusals(list(found.diagnostics)) == [("semantic_models/shared", LINKED)]


def test_a_link_to_a_file_that_is_not_yaml_is_passed_over_as_the_file_would_be(tmp_path: Path) -> None:
    project = tmp_path / "project"
    _write(project / "semantic_models" / "real.yml", METRICS)
    (project / "semantic_models" / "notes.txt").symlink_to(_write(tmp_path / "notes.txt", "notes"))
    assert discover_yaml(project, "semantic_models").diagnostics == ()


def test_a_dangling_yaml_link_is_refused(tmp_path: Path) -> None:
    project = tmp_path / "project"
    _write(project / "semantic_models" / "real.yml", METRICS)
    (project / "semantic_models" / "gone.yml").symlink_to(tmp_path / "missing.yml")
    assert _refusals(list(discover_yaml(project, "semantic_models").diagnostics)) == [
        ("semantic_models/gone.yml", LINKED)
    ]


@pytest.mark.parametrize("target", ["..", ".", "loop"], ids=["parent", "self-dir", "self-link"])
def test_a_link_loop_is_reported_as_a_loop_and_not_walked(tmp_path: Path, target: str) -> None:
    project = tmp_path / "project"
    _write(project / "semantic_models" / "sub" / "real.yml", METRICS)
    (project / "semantic_models" / "sub" / "loop").symlink_to(target)
    found = discover_yaml(project, "semantic_models")
    assert [item.path for item in found.files] == ["semantic_models/sub/real.yml"]
    assert [(item.code, item.context["path"]) for item in found.diagnostics] == [
        ("SST-DIS005", "semantic_models/sub/loop")
    ]


@pytest.mark.parametrize("inside", [True, False], ids=["inside", "outside"])
def test_a_linked_semantic_models_directory_is_refused(tmp_path: Path, inside: bool) -> None:
    project = tmp_path / "project"
    target = (project / "real_models") if inside else (tmp_path / "models")
    _write(target / "a.yml", SMUGGLED_VIEW)
    project.mkdir(exist_ok=True)
    (project / "semantic_models").symlink_to(target)
    with pytest.raises(ProjectError) as raised:
        discover_yaml(project, "semantic_models")
    [diagnostic] = raised.value.diagnostics
    assert diagnostic.message == f"could not read semantic_models: {LINKED}"


def test_a_semantic_models_directory_reached_through_a_linked_folder_is_refused(tmp_path: Path) -> None:
    project = tmp_path / "project"
    _write(tmp_path / "outside" / "models" / "a.yml", SMUGGLED_VIEW)
    project.mkdir()
    (project / "src").symlink_to(tmp_path / "outside")
    with pytest.raises(ProjectError) as raised:
        discover_yaml(project, "src/models")
    [diagnostic] = raised.value.diagnostics
    assert diagnostic.message == "could not read src/models: src is a symbolic link, which SST does not follow"


@pytest.mark.parametrize("configured", ["../models", "ABSOLUTE"])
def test_a_semantic_models_directory_outside_the_project_is_refused(tmp_path: Path, configured: str) -> None:
    project = tmp_path / "project"
    project.mkdir()
    _write(tmp_path / "models" / "a.yml", SMUGGLED_VIEW)
    directory = str(tmp_path / "models") if configured == "ABSOLUTE" else configured
    with pytest.raises(ProjectError) as raised:
        discover_yaml(project, directory)
    [diagnostic] = raised.value.diagnostics
    assert diagnostic.message == f"could not read {directory}: it resolves outside the project"


def test_the_project_itself_may_be_reached_through_a_link(tmp_path: Path) -> None:
    _write(tmp_path / "real" / "semantic_models" / "a.yml", METRICS)
    (tmp_path / "project").symlink_to(tmp_path / "real")
    found = discover_yaml(tmp_path / "project", "semantic_models")
    assert ([item.path for item in found.files], found.diagnostics) == (["semantic_models/a.yml"], ())


# --- agents, tools, and eval metrics --------------------------------------------------------


@pytest.mark.parametrize("linked", ["file", "folder"])
def test_a_linked_agent_file_or_folder_is_refused(tmp_path: Path, linked: str) -> None:
    project = tmp_path / "project"
    _write(project / "agents" / "real" / "agent.yml", "name: real\n")
    outside = _write(tmp_path / "outside" / "agent.yml", "name: smuggled\n")
    if linked == "file":
        (project / "agents" / "evil").mkdir()
        (project / "agents" / "evil" / "agent.yml").symlink_to(outside)
        shown = "it is"
    else:
        (project / "agents" / "evil").symlink_to(outside.parent)
        shown = "agents/evil is"
    agents, diagnostics = load_agents(project)
    assert [agent.name for agent in agents] == ["real"]
    assert _refusals(list(diagnostics)) == [
        ("agents/evil/agent.yml", f"{shown} a symbolic link, which SST does not follow")
    ]


def test_a_linked_tools_file_is_refused(tmp_path: Path) -> None:
    project = tmp_path / "project"
    _write(project / "tools" / "real.yml", "tools: []\n")
    (project / "tools" / "evil.yml").symlink_to(_write(tmp_path / "evil.yml", "tools: []\n"))
    catalog = load_tool_catalog(
        project, DbtCatalog("v12", "1.11", "p", ()), target_name="dev", declared_targets=frozenset(("dev",))
    )
    assert _refusals(list(catalog.diagnostics)) == [("tools/evil.yml", LINKED)]


@pytest.mark.parametrize("linked", ["file", "folder"])
def test_a_linked_eval_metric_file_or_folder_is_refused(tmp_path: Path, linked: str) -> None:
    project = tmp_path / "project"
    outside = _write(tmp_path / "outside" / "judge.yml", "name: smuggled\nscore_ranges: {}\n")
    (project / "eval_metrics").mkdir(parents=True)
    link = project / "eval_metrics" / ("judge.yml" if linked == "file" else "shared")
    link.symlink_to(outside if linked == "file" else outside.parent)
    diagnostics: list[Diagnostic] = []
    assert load_custom_metrics(project, "eval_metrics", diagnostics) == ()
    assert _refusals(diagnostics) == [(link.relative_to(project).as_posix(), LINKED)]


# --- a verified query's file() ------------------------------------------------------------

QUERY = "snowflake_verified_queries:\n  - name: q\n    question: How many?\n    sql: \"{{{{ file('{path}') }}}}\"\n"


def _query_project(tmp_path: Path, sql_file: str) -> Path:
    project = tmp_path / "project"
    _write(project / "semantic_models" / "queries" / "q.yml", QUERY.format(path=sql_file))
    _write(tmp_path / "secret.sql", "SELECT 'secret'\n")
    return project


def _sql_file_results(project: Path) -> tuple[list[tuple[str, str]], list[str]]:
    """The refusals, a path outside the project as SST-REF027 names it, and the SQL each query read."""
    documents = load_documents(discover_yaml(project, "semantic_models"), parse_yaml_bytes)
    queries = load_verified_queries(documents, project, "semantic_models")
    found = list(_verified_query_diagnostics(documents))
    outside = [(item.context["path"], OUTSIDE_PROJECT) for item in coded(found, "SST-REF027")]
    return [*outside, *_refusals(found)], [query.sql for query in queries]


def test_a_sql_file_climbing_out_of_the_project_is_refused_and_never_read(tmp_path: Path) -> None:
    project = _query_project(tmp_path, "../../../secret.sql")
    refusals, sql = _sql_file_results(project)
    assert refusals == [("../../../secret.sql", OUTSIDE_PROJECT)]
    assert sql == []


def test_an_absolute_sql_file_is_refused_and_never_read(tmp_path: Path) -> None:
    absolute = str(tmp_path / "secret.sql")
    refusals, sql = _sql_file_results(_query_project(tmp_path, absolute))
    assert refusals == [(absolute, OUTSIDE_PROJECT)]
    assert sql == []


@pytest.mark.parametrize("target_inside", [True, False], ids=["inside", "outside"])
def test_a_linked_sql_file_is_refused_and_never_read(tmp_path: Path, target_inside: bool) -> None:
    project = _query_project(tmp_path, "q.sql")
    target = _write(project / "sql" / "q.sql", "SELECT 1\n") if target_inside else tmp_path / "secret.sql"
    (project / "semantic_models" / "queries" / "q.sql").symlink_to(target)
    refusals, sql = _sql_file_results(project)
    assert refusals == [("semantic_models/queries/q.sql", LINKED)]
    assert sql == []


def test_a_sql_file_reached_through_a_linked_folder_and_back_up_is_refused(tmp_path: Path) -> None:
    # `link/../x` is taken apart as written, so the file read is the one checked.
    project = _query_project(tmp_path, "link/../../../../secret.sql")
    (tmp_path / "deep" / "a" / "b").mkdir(parents=True)
    (project / "semantic_models" / "queries" / "link").symlink_to(tmp_path / "deep" / "a" / "b")
    refusals, sql = _sql_file_results(project)
    assert refusals == [("link/../../../../secret.sql", OUTSIDE_PROJECT)]
    assert sql == []


def test_a_sql_file_beside_its_document_is_read(tmp_path: Path) -> None:
    project = _query_project(tmp_path, "../sql/q.sql")
    _write(project / "semantic_models" / "sql" / "q.sql", "SELECT 1\n")
    assert _sql_file_results(project) == ([], ["SELECT 1"])


# --- a tool's body: file() -------------------------------------------------------------------


def _routine(path: str) -> str:
    """A tools file declaring procedure `lookup`, whose body is `{{ file('<path>') }}`."""
    return "tool:\n  name: lookup\n  type: procedure\n  body: \"{{ file('" + path + "') }}\"\n"


def _body_results(project: Path) -> tuple[list[tuple[str, str]], list[str], str | None]:
    catalog = load_tool_catalog(
        project, DbtCatalog("v12", "1.11", "p", ()), target_name="dev", declared_targets=frozenset(("dev",))
    )
    [member] = catalog.members
    escapes = [str(item.context["path"]) for item in coded(list(catalog.diagnostics), "SST-REF027")]
    return _refusals(list(catalog.diagnostics)), escapes, member.body


@pytest.mark.parametrize(
    "body",
    ["../../secret.sql", "sql/../../../secret.sql", "{absolute}"],
    ids=["climbing", "climbing-after-a-folder", "absolute"],
)
def test_a_body_file_outside_the_project_is_refused_and_never_read(tmp_path: Path, body: str) -> None:
    # A body's path is relative to the tool file's folder, here tools/.
    project = tmp_path / "project"
    absolute = str(_write(tmp_path / "secret.sql", "SELECT 'secret'\n"))
    written = body.format(absolute=absolute)
    _write(project / "tools" / "routines.yml", _routine(written))
    assert _body_results(project) == ([], [written], None)


@pytest.mark.parametrize("target_inside", [True, False], ids=["inside", "outside"])
def test_a_linked_body_file_is_refused_and_never_read(tmp_path: Path, target_inside: bool) -> None:
    project = tmp_path / "project"
    _write(project / "tools" / "routines.yml", _routine("lookup.sql"))
    target = _write((project if target_inside else tmp_path) / "real.sql", "SELECT 1\n")
    (project / "tools" / "lookup.sql").symlink_to(target)
    assert _body_results(project) == ([("tools/lookup.sql", LINKED)], [], None)


def test_a_body_file_beside_its_tool_file_is_read(tmp_path: Path) -> None:
    project = tmp_path / "project"
    _write(project / "tools" / "ops" / "routines.yml", _routine("sql/lookup.sql"))
    _write(project / "tools" / "ops" / "sql" / "lookup.sql", "SELECT 1\n")
    assert _body_results(project) == ([], [], "SELECT 1\n")


# --- the manifest's input checksums ---------------------------------------------------------


def test_input_checksums_skip_every_link_and_read_nothing_outside_the_project(tmp_path: Path) -> None:
    project = tmp_path / "project"
    _write(project / "tools" / "real.yml", "tools: []\n")
    _write(project / "agents" / "a" / "agent.yml", "name: a\n")
    outside = _write(tmp_path / "outside" / "secret.yml", "tools: []\n")
    (project / "tools" / "evil.yml").symlink_to(outside)
    (project / "tools" / "inside.yml").symlink_to(project / "tools" / "real.yml")
    (project / "tools" / "shared").symlink_to(outside.parent)
    (project / "eval_metrics").symlink_to(outside.parent)
    checksums = _file_checksums(ProjectPaths(project, None), {})
    assert sorted(checksums) == ["agents/a/agent.yml", "tools/real.yml"]
