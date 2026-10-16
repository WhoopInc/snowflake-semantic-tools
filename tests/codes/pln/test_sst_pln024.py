"""SST-PLN024: an object holds the target name and state records no SST entry for it."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.lifecycle.evals import EvalLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, ChangeReason, OwnershipMarker
from tests.helpers.artifact_builders import empty_state, target
from tests.helpers.compile_builders import compiled_as
from tests.helpers.diagnostic_filters import coded
from tests.helpers.eval_builders import compile_eval, seed_dataset_version
from tests.helpers.plan_codes import entry, live, manifest_of, plan, view
from tests.helpers.snowflake_fake import FakeSnowflake


def test_sst_pln024_fires() -> None:
    sales = view("sales")
    planned = plan((sales,), observed=(live(sales),))
    [diagnostic] = coded(planned.diagnostics, "SST-PLN024")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:sales: DB.SCH.SALES exists without trusted SST ownership"
    assert planned.changes[0].reason is ChangeReason.UNMANAGED_OBJECT


def test_sst_pln024_silent() -> None:
    sales = view("sales")
    planned_manifest = manifest_of(sales)
    marked = live(sales, marker=OwnershipMarker(planned_manifest.manifest_id, sales.fingerprint))
    planned = plan(
        (sales,),
        observed=(marked,),
        applied={sales.key: entry(sales, planned_manifest.manifest_id)},
        manifest=planned_manifest,
    )
    assert coded(planned.diagnostics, "SST-PLN024") == [] and planned.changes[0].action is Action.NOOP


def test_sst_pln024_fires_for_an_eval_dataset_whose_version_carries_another_provenance() -> None:
    # An apply that stopped after minting leaves SST's provenance in the version's METADATA, and
    # the dataset is adopted; one that names another agent stays someone else's.
    result = compile_eval()
    manifest = build_manifest(result)
    artifact = compiled_as(result, CompiledEval).rendered_for_publish(manifest.manifest_id)
    port = FakeSnowflake(existing=tuple(name.sql for _, name in artifact.physical_resources))
    seed_dataset_version(artifact, port)
    dataset = next(name for kind, name in artifact.physical_resources if kind == "DATASET")
    version = dict(artifact.component_fingerprints)["dataset_version"]
    port.dataset_version_metadata_text[(dataset.sql, version)] = '{"agent":"DB.S.SOMEONE_ELSE"}'
    planned = PlanArtifacts(port, lifecycle_handlers={"eval": EvalLifecycleHandler(port)}).run(
        {artifact.key: artifact}, manifest, empty_state(), target(), fetched_at="now"
    )
    [change] = planned.changes
    assert [item.code for item in change.diagnostics] == ["SST-PLN024"]
    assert change.reason is ChangeReason.UNMANAGED_OBJECT
