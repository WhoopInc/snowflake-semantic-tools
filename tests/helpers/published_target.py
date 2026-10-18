"""A fake target the reference project publishes onto in full, so a plan after apply finds it all.

`FakeSnowflake` publishes the composite artifacts itself -- extensions, stages and their files,
the profile registry, eval datasets -- but no statement leaves an object SHOW lists. On
`PublishingSnowflake`, a script that succeeds and names a compiled semantic view, tool or agent
leaves that object listed under its compiled name with the marker apply writes, and each eval's
source table counts its questions, so the next plan reads every artifact as unchanged.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

from snowflake_semantic_tools.adapters.locations import locate_project
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.cli.wiring.compile import compile_result
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, OwnershipMarker, QueryResult, ShowRow
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.reference_project import DBT_MANIFEST
from tests.helpers.snowflake_fake import FakeSnowflake, Sent

# The object type SHOW lists each marked artifact type under, in the reference project.
SHOWN_AS = {"semantic_view": "SEMANTIC VIEW", "tool": "CORTEX SEARCH SERVICE", "agent": "AGENT"}
_ROW_COUNT = "SELECT COUNT(*) AS ROW_COUNT FROM "
_ROWS = "SELECT INPUT_QUERY, GROUND_TRUTH FROM "


class PublishingSnowflake(FakeSnowflake):
    """The fake account, on which a published semantic view, tool or agent is listed with its marker.

    Attributes:
        manifest_id: The manifest apply publishes, which every marker names.
        marked: `(artifact type, fingerprint)` of each marked artifact, by its compiled target.
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
            for name, (artifact_type, fingerprint) in self.marked.items():
                if name in text:
                    self._list(name, SHOWN_AS[artifact_type], fingerprint)
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


def publishing_port(project: Path, target: str = "dev") -> PublishingSnowflake:
    """Return an empty fake target for `project`, compiled, where every relation its artifacts read exists."""
    manifest = json.loads((project / "target" / "sst" / "manifest.json").read_text(encoding="utf-8"))
    marked = {
        value["publish_target"]["qualified_name"]: (key.split(":", 1)[0], value["fingerprint"])
        for key, value in manifest["artifacts"].items()
        if key.split(":", 1)[0] in SHOWN_AS
    }
    compiled = compile_result(locate_project(project), target, DBT_MANIFEST)
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
