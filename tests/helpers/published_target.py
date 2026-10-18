"""A fake target the reference project publishes onto in full, so a plan after apply finds it all.

`FakeSnowflake` publishes the composite artifacts itself -- extensions, stages and their files,
the profile registry, eval datasets -- but no statement leaves an object SHOW lists. On
`PublishingSnowflake`, a script that succeeds and names a compiled semantic view, tool or agent
leaves that object listed under its compiled name with the marker apply writes, and each eval's
source table counts its questions, so the next plan reads every artifact as unchanged.

`published` makes such a target and has its views, search services and, when asked, agents
DESCRIBE as published, so a plan compares updates member by member.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.adapters.locations import locate_project
from snowflake_semantic_tools.app.compile import CompiledView
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.compile.tools import CompiledTool
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.wiring.compile import compile_result
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, OwnershipMarker, QueryResult, ShowRow
from snowflake_semantic_tools.domain.plan.definitions.tool import search_query
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.cli_projects import common
from tests.helpers.described_objects import described_routine, described_search, described_view
from tests.helpers.reference_project import DBT_MANIFEST
from tests.helpers.snowflake_fake import FakeSnowflake, Sent

# The artifact types apply marks with the object they publish, which SHOW then lists.
MARKED_TYPES = frozenset(("semantic_view", "tool", "agent"))
_ROW_COUNT = "SELECT COUNT(*) AS ROW_COUNT FROM "
_ROWS = "SELECT INPUT_QUERY, GROUND_TRUTH FROM "


class PublishingSnowflake(FakeSnowflake):
    """The fake account, on which a published semantic view, tool or agent is listed with its marker.

    Attributes:
        manifest_id: The manifest apply publishes, which every marker names.
        marked: `(object type, fingerprint)` of each marked artifact, by its compiled target.
        questions: Each eval source table's question count, which COUNT(*) answers.
        rows: Each eval source table's questions and ground truths, which reading it answers.
    """

    def __init__(
        self,
        manifest_id: str,
        marked: dict[str, tuple[str, str]],
        questions: dict[str, int],
        existing: set[str],
        rows: dict[str, tuple[tuple[str, str], ...]] | None = None,
    ) -> None:
        super().__init__(state={}, existing=tuple(existing))
        self.manifest_id = manifest_id
        self.marked = marked
        self.questions = questions
        self.rows = rows or {}

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        result = super().execute_script(statements)
        if result.ok:
            text = "\n".join(str(statement) for statement in statements)
            for name, (object_type, fingerprint) in self.marked.items():
                if name in text:
                    self._list(name, object_type, fingerprint)
        return result

    def query(self, sql: Sql, params: object = None) -> QueryResult:
        text = str(sql)
        table = text[len(_ROW_COUNT) :] if text.startswith(_ROW_COUNT) else ""
        if table in self.questions:
            self.log.append(Sent("query", (text,), params))
            return QueryResult(("ROW_COUNT",), ((self.questions[table],),))
        read = text[len(_ROWS) :] if text.startswith(_ROWS) else ""
        if read in self.rows:
            self.log.append(Sent("query", (text,), params))
            return QueryResult(("INPUT_QUERY", "GROUND_TRUTH"), self.rows[read])
        return super().query(sql, params)

    def _list(self, name: str, object_type: str, fingerprint: str) -> None:
        marker = OwnershipMarker(self.manifest_id, fingerprint)
        self.markers[name] = marker
        database, schema, object_name = name.split(".")
        key = (object_type, f"{database}.{schema}")
        kept = tuple(row for row in self.objects.get(key, ()) if row.name != object_name)
        row = ShowRow(object_name, database, schema, self.role, "now", marker.text, object_type=object_type)
        self.objects[key] = (*kept, row)


def object_types(project: Path, target: str = "dev") -> dict[str, str]:
    """Return, by artifact key, the object type each artifact `project` compiles for `target` becomes."""
    compiled = compile_result(locate_project(project), target, DBT_MANIFEST)
    return {item.artifact_key: item.rendered_artifact.object_type for item in compiled.compiled}


def publishing_port(project: Path, target: str = "dev") -> PublishingSnowflake:
    """Return an empty fake target for `project`, compiled, where every relation its artifacts read exists."""
    manifest = json.loads((project / "target" / "sst" / "manifest.json").read_text(encoding="utf-8"))
    compiled = compile_result(locate_project(project), target, DBT_MANIFEST)
    shown = object_types(project, target)
    marked = {
        value["publish_target"]["qualified_name"]: (shown[key], value["fingerprint"])
        for key, value in manifest["artifacts"].items()
        if key.split(":", 1)[0] in MARKED_TYPES
    }
    existing = {relation.sql for item in compiled.compiled for relation in item.rendered_artifact.required_relations}
    evals = [item for item in compiled.compiled if isinstance(item, CompiledEval)]
    questions = {item.source_table.sql: len(item.resolved.dataset.questions) for item in evals}
    rows = {
        item.source_table.sql: tuple(
            (str(row["input_query"]), json.dumps(row["ground_truth"]))
            for row in json.loads(item.rendered.dataset_payload)
        )
        for item in evals
    }
    return PublishingSnowflake(manifest["manifest_id"], marked, questions, existing, rows)


def describe_published(port: PublishingSnowflake, project: Path, target: str, *, agents: bool = False) -> None:
    """Make each view, search service and routine `target` compiles DESCRIBE as published, and each agent with `agents`.

    Each view's GET_DDL returns its render, so a plan's drift check reads it.

    With `agents`, each agent serves one version, the default, holding the specification published;
    the fake then reads each agent's versions as drifted, so a plan updates every agent.
    """
    for item in compile_result(locate_project(project), target, DBT_MANIFEST).compiled:
        name = item.rendered_artifact.target.sql
        if isinstance(item, CompiledView):
            port.view_descriptions[name] = described_view(item.view)
            port.definitions[name] = item.rendered_artifact.ddl
        elif isinstance(item, CompiledAgent) and agents:
            alias = item.rendered_artifact.desired_alias
            port.agent_version_rows[name] = (AgentVersion("VERSION$1", alias, True, specification=item.payload),)
        elif isinstance(item, CompiledTool) and item.member.type == "cortex_search_service":
            described = described_search(item.member, search_query(item.rendered.ddl))
            port.descriptions[f"CORTEX SEARCH SERVICE {name}"] = described
    describe_routines(port, project, target)


def describe_routines(port: FakeSnowflake, project: Path, target: str = "dev") -> None:
    """Make each procedure and function `target` compiles DESCRIBE as published."""
    for item in compile_result(locate_project(project), target, DBT_MANIFEST).compiled:
        if isinstance(item, CompiledTool) and item.rendered.object_type in ("PROCEDURE", "FUNCTION"):
            types = ", ".join(parameter.type for parameter in item.member.signature)
            key = f"{item.rendered.object_type} {item.rendered.target.sql}({types})"
            port.routine_descriptions[key] = described_routine(item.member)


def published(
    project: Path, patch: pytest.MonkeyPatch, target: str = "dev", *, agents: bool = False
) -> PublishingSnowflake:
    """Compile `project` for `target`, apply it to a fresh fake account, and have what it holds DESCRIBE."""
    result = CliRunner().invoke(cli, ["compile", *common(project), "--target", target])
    assert result.exit_code == 0, result.output
    port = publishing_port(project, target)
    port.close = lambda: None  # type: ignore[attr-defined]
    patch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", lambda params: port)
    applied = CliRunner().invoke(cli, ["apply", *common(project), "--target", target, "--yes"])
    assert applied.exit_code == 0, applied.output
    describe_published(port, project, target, agents=agents)
    return port
