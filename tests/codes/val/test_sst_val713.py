"""SST-VAL713: SST must add its version to a dataset that the session's role does not own.

A publish that stopped after minting leaves the dataset without SST's version; if its owner
then changed, the next plan refuses, since only the owner may add a version.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.lifecycle.evals import EVAL_STAGE_FILE_FORMAT, EvalLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, GrantRow
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.app_ports import InMemorySnowflake
from tests.helpers.compile_builders import compiled_as
from tests.helpers.eval_builders import compile_eval


def _plan(grants: tuple[GrantRow, ...]) -> tuple[Action, list[str], list[str]]:
    result = compile_eval()
    manifest = build_manifest(result)
    artifact = compiled_as(result, CompiledEval).rendered_for_publish(manifest.manifest_id)
    port = InMemorySnowflake()
    port.existing = {name.sql for _, name in artifact.physical_resources} | {"DB.S.EVAL_CONFIGS"}
    port.stage_formats["DB.S.EVAL_CONFIGS"] = EVAL_STAGE_FILE_FORMAT
    dataset = dict(artifact.physical_resources)["DATASET"]
    port.grants[dataset.sql] = grants
    entry = AppliedEntry(
        artifact.fingerprint,
        artifact.target.sql,
        "now",
        "run",
        "failed_after_write",
        artifact.fingerprint,
        manifest.manifest_id,
        component_fingerprints=artifact.component_fingerprints,
        physical_resources=tuple((kind, name.sql) for kind, name in artifact.physical_resources),
    )
    planned = EvalLifecycleHandler(port).plan(artifact, entry, manifest)
    return (
        planned.action,
        [item.code for item in planned.diagnostics],
        [item.message for item in planned.diagnostics],
    )


def test_sst_val713_fires() -> None:
    action, codes, messages = _plan((GrantRow("OWNERSHIP", "ROLE", "ADMIN"), GrantRow("USAGE", "ROLE", "TEST_ROLE")))
    assert action is Action.BLOCKED
    assert codes == ["SST-VAL713"]
    assert messages == ["dataset 'eval:sales_agent': TEST_ROLE holds USAGE, not OWNERSHIP"]
    assert ERROR_REGISTRY["SST-VAL713"].severity is Severity.ERROR


def test_sst_val713_silent() -> None:
    action, codes, _ = _plan((GrantRow("OWNERSHIP", "ROLE", "TEST_ROLE"),))
    assert action is Action.UPDATE
    assert "SST-VAL713" not in codes
