"""SST-APL028: an eval config stage SST did not create has the wrong FILE FORMAT."""

from __future__ import annotations

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.lifecycle.evals import EVAL_STAGE_FILE_FORMAT, EvalLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.compile_builders import compiled_as
from tests.helpers.eval_builders import compile_eval
from tests.helpers.snowflake_fake import FakeSnowflake

WRONG = "TYPE='CSV' FIELD_DELIMITER=','"


def planned(file_format: str) -> tuple[Diagnostic, ...]:
    assert ApplyArtifacts is not None  # apply first: the lifecycle modules import it
    result = compile_eval()
    manifest = build_manifest(result)
    artifact = compiled_as(result, CompiledEval).rendered_for_publish(manifest.manifest_id)
    port = FakeSnowflake()
    port.existing = {"DB.S.EVAL_CONFIGS"}
    port.stage_formats["DB.S.EVAL_CONFIGS"] = file_format
    return tuple(EvalLifecycleHandler(port).plan(artifact, None, manifest).diagnostics)


def test_sst_apl028_fires() -> None:
    [diagnostic] = [item for item in planned(WRONG) if item.code == "SST-APL028"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        f"eval config stage 'DB.S.EVAL_CONFIGS': FILE FORMAT is {WRONG}, expected {EVAL_STAGE_FILE_FORMAT}"
    )


def test_sst_apl028_silent() -> None:
    assert "SST-APL028" not in [item.code for item in planned(EVAL_STAGE_FILE_FORMAT)]
