"""SST-VAL715: a dataset version's METADATA or COMMENT holds personal or regulated data.

SST builds METADATA from object names and hex digests, and COMMENT from the version's hex,
so authored text never reaches either; plan checks both before it adds the version, so a
change that let it in is reported rather than published.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.lifecycle.evals import EvalLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, RenderedArtifact
from snowflake_semantic_tools.domain.validate.publication import dataset_metadata_diagnostics
from tests.helpers.eval_builders import compile_eval
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.val_codes import eval_artifact


def _plan(artifact: RenderedArtifact) -> tuple[Action, list[str]]:
    port = FakeSnowflake()
    port.existing = set()
    planned = EvalLifecycleHandler(port).plan(artifact, None, build_manifest(compile_eval()))
    return planned.action, [item.code for item in planned.diagnostics]


def test_sst_val715_fires() -> None:
    metadata = '{"agent":"DB.S.AGENT","contact":"ana@example.com"}'
    [diagnostic, unknown] = dataset_metadata_diagnostics("eval:sales_agent", metadata=metadata, comment="SST")
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-VAL715",
        Severity.ERROR,
        "eval:sales_agent",
    )
    assert diagnostic.message == "dataset 'eval:sales_agent': METADATA matches an email address"
    assert unknown.message == "dataset 'eval:sales_agent': METADATA matches a field SST never writes ('contact')"


def test_sst_val715_blocks_the_plan_that_would_add_the_version() -> None:
    leaked = replace(eval_artifact(), version_metadata='{"agent":"DB.S.AGENT","git_sha":"123-45-6789"}')
    assert _plan(leaked) == (Action.BLOCKED, ["SST-VAL715"])


def test_sst_val715_silent() -> None:
    action, codes = _plan(eval_artifact())
    assert action is Action.CREATE
    assert "SST-VAL715" not in codes
