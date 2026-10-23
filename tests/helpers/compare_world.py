"""A published agent or CoWork profile in the fake, as `sst apply` leaves it, for the comparison tests.

`compare_subject` renders a subject as plan publishes it: staged under `DB.S.AGENT_SPECS`, its
specification on `claude-sonnet-4-6`, with the alias `RELEASED`. `publish` stages it on a fake
whose versions each ALTER AGENT changes: listed with its marker, released as `VERSION$4` with the
alias and the default pinned there by name, a live version holding the same specification, and
the state entry apply recorded. A plan of the subject is then NOOP. `ComparingSnowflake` also
answers every evaluation run a comparison starts.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import replace

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.compile.staged_agent import (
    AgentObjectMetadata,
    StagedAgentObject,
    render_agent_object,
    staged_path,
)
from snowflake_semantic_tools.app.evals.compare_lifecycle import CompareSubject
from snowflake_semantic_tools.app.evals.retrieve import compiled_questions
from snowflake_semantic_tools.domain.model.agent import AgentProfile
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import (
    OwnershipMarker,
    QueryResult,
    ShowRow,
    marked_comment,
    spec_fingerprint,
    version_comment,
)
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from snowflake_semantic_tools.domain.render.agent import render_spec_json
from snowflake_semantic_tools.domain.sql import Sql
from snowflake_semantic_tools.domain.state import APPLIED, AppliedEntry
from tests.helpers.eval_builders import RESULT_COLUMNS, STATUS_COLUMNS, result_row, spans
from tests.helpers.snowflake_fake import FakeSnowflake, Sent

STAGE = QualifiedName.parse("DB.S.AGENT_SPECS")
MANIFEST_ID = "a" * 64
GIT_SHA = "abc1234"
PINNED = "claude-sonnet-4-6"
RELEASE = "VERSION$4"
ALIAS = "RELEASED"


def payload(model: str = PINNED) -> str:
    """The subject's specification, orchestrated by `model`."""
    return render_spec_json({"models": {"orchestration": model}, "instructions": {"response": "Be brief."}})


def compare_subject(
    name: str = "sales_agent", *, kind: str = "agent", target: str = "DB.S.SALES_AGENT", alias: str | None = ALIAS
) -> CompareSubject:
    """Render one subject as plan publishes it, and describe it as a comparison commits versions of it."""
    qualified = QualifiedName.parse(target)
    folder = name if kind == "agent" else f"cowork-profiles/{name}"
    spec = payload()
    staged = StagedAgentObject(
        key=f"{kind}:{name}",
        artifact_type=kind,
        target=qualified,
        payload=spec,
        definition_fingerprint="d" * 64,
        spec_fingerprint=spec_fingerprint(spec),
        metadata=AgentObjectMetadata(alias=alias, tags=(), profile=AgentProfile(), secure=None),
        depends_on=(),
        references=(),
        folder=folder,
        stage_path=staged_path(STAGE, folder, GIT_SHA),
        stage=STAGE,
    )
    marker = OwnershipMarker(MANIFEST_ID, "d" * 64)
    rendered = render_agent_object(staged, marked_comment(None, marker.text))
    return CompareSubject(
        f"{kind}:{name}", kind, name, qualified, spec, PINNED, STAGE, folder, replace(rendered, expected_marker=marker)
    )


def publish(port: FakeSnowflake, subject: CompareSubject, *, earlier: tuple[AgentVersion, ...] = ()) -> AppliedEntry:
    """Stage the subject on `port` as apply leaves it, its versions modeled; return the state entry.

    Args:
        earlier: Versions an earlier comparison committed, listed after the release.
    """
    target = subject.target
    port.modeled_agents.add(target.sql)
    port.show(
        ShowRow(
            target.name.value,
            target.database.value,
            target.schema.value,
            "OWNER",
            "now",
            comment=marked_comment(None, OwnershipMarker(MANIFEST_ID, "d" * 64).text),
            object_type="AGENT",
        )
    )
    port.agent_version_rows[target.sql] = (
        AgentVersion(None, None, specification=subject.payload),
        *(AgentVersion(f"VERSION${number}", None, comment=version_comment(GIT_SHA, "{}")) for number in (1, 2, 3)),
        AgentVersion(
            RELEASE,
            ALIAS if subject.rendered.desired_alias else None,
            True,
            comment=version_comment(GIT_SHA, subject.payload),
            specification=subject.payload,
        ),
        *earlier,
    )
    port.agent_default_settings[target.sql] = RELEASE
    return AppliedEntry("d" * 64, target.sql, "now", "run", APPLIED, "ddl", MANIFEST_ID, GIT_SHA)


def orchestration(specification: str | None) -> object:
    """Return the orchestration model a version's specification names."""
    return json.loads(specification or "{}").get("models", {}).get("orchestration")


class ComparingSnowflake(FakeSnowflake):
    """The fake on which every evaluation run of a comparison completes at once, scored by its model.

    A START is logged as a script `("IN <scope>", <statement>)`; a STATUS reads COMPLETED for the
    run it names; the results of a run score each question `scores[<model slug>]`, else 1.0, and
    name request `<run>-q<n>`; its router spans route each question to `DB.AGENTS.SALES`.
    `failing` names the model slugs whose runs end FAILED.

    Attributes:
        compiled: The compiled evals whose questions the results answer, by agent name, upper-cased.
    """

    def __init__(self, compiled: Sequence[CompiledEval], *, scores: Mapping[str, float] | None = None) -> None:
        super().__init__()
        self.compiled = {item.agent_target.name.value.upper(): item for item in compiled}
        self.scores = dict(scores or {})
        self.failing: set[str] = set()
        self.started: list[str] = []

    def query_in_context(
        self, scope: SchemaScope, sql: Sql, params: object = None, *, timeout_seconds: int | None = None
    ) -> QueryResult:
        text = str(sql)
        if "EXECUTE_AI_EVALUATION('START'" in text:
            self.log.append(Sent("script", (f"IN {scope.sql}", text)))
            self.started.append(text.split("'run_name', '", 1)[1].split("'", 1)[0])
            return QueryResult()
        values = tuple(params) if isinstance(params, (tuple, list)) else ()
        if "EXECUTE_AI_EVALUATION('STATUS'" in text:
            run = str(values[0])
            agent = self._agent(run)
            status = "FAILED" if any(slug.upper() in run for slug in self.failing) else "COMPLETED"
            return QueryResult(STATUS_COLUMNS, ((run, agent, "CORTEX AGENT", status, []),))
        if "GET_AI_EVALUATION_DATA" in text:
            return self._results(str(values[4]))
        if "AI_OBSERVABILITY_EVENTS" in text:
            return self._spans(str(values[0]))
        return super().query_in_context(scope, sql, params, timeout_seconds=timeout_seconds)

    def _agent(self, run: str) -> str:
        return next(name for name in self.compiled if run.startswith(f"{name}_CMP_"))

    def _results(self, run: str) -> QueryResult:
        compiled = self.compiled[self._agent(run)]
        score = next((value for slug, value in self.scores.items() if f"_{slug.upper()}_" in run), 1.0)
        rows = []
        for index, (_, query, truth) in enumerate(compiled_questions(compiled), start=1):
            for metric, kind in _metric_kinds(compiled):
                rows.append(
                    result_row(
                        metric,
                        score,
                        record_id=f"{run}-r{index}",
                        request_id=f"{run}-q{index}",
                        input_query=query,
                        ground_truth=dict(truth),
                        metric_type=kind,
                    ).rows[0]
                )
        return QueryResult(RESULT_COLUMNS, tuple(rows))

    def _spans(self, run: str) -> QueryResult:
        compiled = self.compiled[self._agent(run)]
        records = [(f"{run}-r{index}", "DB.AGENTS.SALES") for index in range(1, len(compiled_questions(compiled)) + 1)]
        return spans(*records)


def _metric_kinds(compiled: CompiledEval) -> list[tuple[str, str]]:
    system = [(str(metric.name), "system") for metric in compiled.resolved.config.system_metrics if metric.name]
    return [*system, *((metric.name, "custom") for metric in compiled.resolved.custom_metrics if metric.enabled)]
