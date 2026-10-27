"""SST-VAL714: the dataset version a plan adds carries no commit in its METADATA.

The eval API cannot select a dataset version, so METADATA is the only place the dataset's
provenance lives. Outside a git checkout the commit is unknown, and plan warns before it
mints a dataset that cannot say where its questions came from.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.lifecycle.evals import EvalLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action
from tests.helpers.compile_builders import compiled_as
from tests.helpers.eval_builders import compile_eval
from tests.helpers.snowflake_fake import FakeSnowflake


def _create_plan(git_sha: str) -> tuple[Action, list[tuple[str, Severity, str, str | None]]]:
    result = compile_eval(git_sha=git_sha)
    manifest = build_manifest(result)
    artifact = compiled_as(result, CompiledEval).rendered_for_publish(manifest.manifest_id)
    port = FakeSnowflake()
    port.existing = set()
    planned = EvalLifecycleHandler(port).plan(artifact, None, manifest)
    return planned.action, [(item.code, item.severity, item.message, item.subject) for item in planned.diagnostics]


def test_sst_val714_fires() -> None:
    # What `sst plan` stamps when git cannot name the commit.
    action, found = _create_plan("WORKTREE")
    assert action is Action.CREATE
    assert found == [
        ("SST-VAL714", Severity.WARNING, "dataset 'eval:sales_agent': METADATA has no git SHA", "eval:sales_agent")
    ]


def test_sst_val714_silent() -> None:
    action, found = _create_plan("abc1234")
    assert action is Action.CREATE
    assert found == []
