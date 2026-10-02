"""SST-APL022: Snowflake rejected the statement that adds an eval's dataset version."""

from __future__ import annotations

from collections.abc import Sequence

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.lifecycle.evals import EVAL_STAGE_FILE_FORMAT, EvalLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.plan import PlanArtifacts
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import ApplyResult, ExecResult, ExecutionError
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.app_ports import FixedClock, InMemorySnowflake, InMemoryStateStore
from tests.helpers.apply_runs import STATE_TABLE
from tests.helpers.artifact_builders import state, target
from tests.helpers.compile_builders import compiled_as
from tests.helpers.eval_builders import compile_eval


def publish(port: InMemorySnowflake) -> ApplyResult:
    result = compile_eval()
    manifest = build_manifest(result)
    artifact = compiled_as(result, CompiledEval).rendered_for_publish(manifest.manifest_id)
    port.existing = {"DB.S.EVAL_CONFIGS"}
    port.stage_formats["DB.S.EVAL_CONFIGS"] = EVAL_STAGE_FILE_FORMAT
    handler = EvalLifecycleHandler(port)
    plan = PlanArtifacts(port, lifecycle_handlers={"eval": handler}).run(
        {artifact.key: artifact}, manifest, state(), target(), fetched_at="now"
    )
    use_case = ApplyArtifacts(
        port, InMemoryStateStore(), FixedClock(), state_table=STATE_TABLE, lifecycle_handlers={"eval": handler}
    )
    return use_case.run(plan, state())


class DatasetRefused(InMemorySnowflake):
    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        if "SYSTEM$CREATE_EVALUATION_DATASET" in " ".join(str(item) for item in statements):
            return ExecResult(False, error=ExecutionError("Insufficient privileges to operate on dataset", "42501"))
        return super().execute_script(statements)


def test_sst_apl022_fires() -> None:
    [diagnostic] = [item for item in publish(DatasetRefused()).diagnostics if item.code == "SST-APL022"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "dataset 'eval:sales_agent': ADD VERSION failed: Insufficient privileges to operate on dataset"
    )


def test_sst_apl022_silent() -> None:
    assert "SST-APL022" not in [item.code for item in publish(InMemorySnowflake()).diagnostics]
