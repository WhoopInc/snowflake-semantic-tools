"""SST-VAL744: a custom metric's prompt was edited while it kept the name state recorded it under.

A custom metric has no Snowflake version: its name is the score's column and chart. The eval's
state entry records each metric's definition digest, so the next plan refuses an edit that
would keep the name; versioning a metric means giving it a new one.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.lifecycle.evals import EvalLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from snowflake_semantic_tools.domain.model.eval import ResolvedEval
from snowflake_semantic_tools.domain.model.lifecycle import Action, RenderedArtifact
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.compile_builders import compiled_as
from tests.helpers.eval_builders import compile_eval, resolved_eval
from tests.helpers.snowflake_fake import FakeSnowflake


def _published(resolved: ResolvedEval) -> RenderedArtifact:
    result = compile_eval(resolved)
    return compiled_as(result, CompiledEval).rendered_for_publish(build_manifest(result).manifest_id)


def _entry(artifact: RenderedArtifact) -> AppliedEntry:
    return AppliedEntry(
        artifact.fingerprint,
        artifact.target.sql,
        "now",
        "run",
        "applied",
        artifact.fingerprint,
        "manifest",
        component_fingerprints=artifact.component_fingerprints,
    )


def _with_prompt(prompt: str, *, name: str = "grounding") -> ResolvedEval:
    value = resolved_eval()
    metric = replace(value.custom_metrics[0], name=name, prompt=prompt)
    config = replace(value.config, custom_metric_names=(name,))
    return replace(value, config=config, custom_metrics=(metric,))


def _plan(before: ResolvedEval, after: ResolvedEval) -> tuple[Action, list[tuple[str, str, str | None]]]:
    previous = _published(before)
    result = compile_eval(after)
    current = compiled_as(result, CompiledEval).rendered_for_publish(build_manifest(result).manifest_id)
    port = FakeSnowflake()
    port.existing = set()
    planned = EvalLifecycleHandler(port).plan(current, _entry(previous), build_manifest(result))
    return planned.action, [(item.code, item.message, item.subject) for item in planned.diagnostics]


def test_sst_val744_fires() -> None:
    action, found = _plan(resolved_eval(), _with_prompt("Score from 0 to 5. Penalise any unsourced claim."))
    assert action is Action.BLOCKED
    [(code, message, subject)] = found
    assert code == "SST-VAL744"
    assert message == "eval metric 'grounding' declares no version"
    assert subject == "eval_metric:grounding"
    assert ERROR_REGISTRY["SST-VAL744"].severity is Severity.ERROR


def test_sst_val744_silent() -> None:
    # The same edit under a new name is a new metric, which is how a metric is versioned.
    action, found = _plan(resolved_eval(), _with_prompt("Score from 0 to 5. Penalise.", name="grounding_v2"))
    assert "SST-VAL744" not in [code for code, _, _ in found]
    assert action is not Action.BLOCKED
