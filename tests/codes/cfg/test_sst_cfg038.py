"""SST-CFG038: `sst enrich` is asked to read row data, and the project refuses sample-value collection."""

from __future__ import annotations

from snowflake_semantic_tools.app.enrich import EnrichProject, EnrichRequest
from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from snowflake_semantic_tools.domain.enrich import Component, resolve_options
from tests.helpers.enrich_ports import InMemoryFiles, ScriptedEnrich
from tests.helpers.project_inputs import InMemoryProjectInputs


def _run(allow: bool, *included: Component) -> list[str]:
    inputs = InMemoryProjectInputs(tree={"enrichment": {"allow_sample_value_collection": allow}})
    project = EnrichProject(inputs, ScriptedEnrich(), InMemoryFiles({}))
    report = project.run(EnrichRequest(options=resolve_options(frozenset(included), frozenset())))
    return [item.code for item in report.diagnostics]


def test_sst_cfg038_fires() -> None:
    inputs = InMemoryProjectInputs(tree={"enrichment": {"allow_sample_value_collection": False}})
    report = EnrichProject(inputs, ScriptedEnrich(), InMemoryFiles({})).run(
        EnrichRequest(options=resolve_options(frozenset((Component.SAMPLE_VALUES,)), frozenset()))
    )
    [diagnostic] = report.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG038", Severity.ERROR)
    assert diagnostic.message.endswith("reads row data, and enrichment.allow_sample_value_collection is false")
    assert not ERROR_REGISTRY["SST-CFG038"].demotable and report.stopped


def test_sst_cfg038_silent() -> None:
    assert "SST-CFG038" not in _run(True, Component.SAMPLE_VALUES)
    assert "SST-CFG038" not in _run(False, Component.COLUMN_TYPES)
