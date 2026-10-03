"""SST-VAL716: a run config declares the dataset it runs against, and that dataset already exists.

SST creates the dataset itself, so the config it renders holds `evaluation:` and `metrics:`
only; plan checks the config against the dataset it observed, so a `dataset:` block that
would ask every run to create the dataset again is reported rather than staged.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.lifecycle.evals import EvalLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, RenderedArtifact
from snowflake_semantic_tools.domain.validate.publication import run_config_diagnostics
from tests.helpers.compile_builders import compiled_as
from tests.helpers.eval_builders import compile_eval
from tests.helpers.snowflake_fake import FakeSnowflake

CONFIG = "evaluation:\n  agent_params: {}\nmetrics:\n  - name: correctness\n"


def _artifact() -> RenderedArtifact:
    result = compile_eval()
    return compiled_as(result, CompiledEval).rendered_for_publish(build_manifest(result).manifest_id)


def _plan(artifact: RenderedArtifact) -> tuple[Action, list[str]]:
    port = FakeSnowflake()
    # The dataset exists, so the plan sees it as it would on any run after the first.
    port.existing = {name.sql for kind, name in artifact.physical_resources if kind == "DATASET"}
    planned = EvalLifecycleHandler(port).plan(artifact, None, build_manifest(compile_eval()))
    return planned.action, [item.code for item in planned.diagnostics]


def test_sst_val716_fires() -> None:
    [diagnostic] = run_config_diagnostics("eval:sales_agent", CONFIG + "dataset:\n  name: D\n", dataset_exists=True)
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-VAL716",
        Severity.ERROR,
        "eval:sales_agent",
    )
    assert diagnostic.message == "dataset 'eval:sales_agent' emits a dataset: block and the object already exists"


def test_sst_val716_blocks_the_plan_whose_config_declares_the_dataset() -> None:
    artifact = _artifact()
    assert _plan(replace(artifact, ddl=artifact.ddl + "dataset:\n  name: D\n")) == (Action.BLOCKED, ["SST-VAL716"])


def test_sst_val716_silent() -> None:
    # The config SST renders, against the dataset that exists: no dataset: block to report.
    assert "SST-VAL716" not in _plan(_artifact())[1]
    assert run_config_diagnostics("eval:sales_agent", CONFIG + "dataset:\n", dataset_exists=False) == ()
