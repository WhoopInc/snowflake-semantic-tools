"""A CoWork profile's eval read from disk: its `evals:` block, its dataset's expected subagents,
and its config's routing metrics.

CoWork profiles are a Snowflake private preview; a profile's eval files sit in the `evals/`
folder beside its `cowork_profile.yml`, read as an agent's are.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.project_source import YamlProjectSource
from snowflake_semantic_tools.adapters.yaml.cowork_profiles import load_cowork_profiles
from snowflake_semantic_tools.adapters.yaml.evals import load_eval_catalog
from snowflake_semantic_tools.adapters.yaml.routes import routed_diagnostics
from snowflake_semantic_tools.domain.model.eval import EvalCatalog
from tests.helpers.eval_projects import write
from tests.helpers.projects import project_paths
from tests.helpers.reference_project import DBT_MANIFEST, project_copy, with_profile_eval

PROFILE = "cowork_profiles/concierge"
DATASET = """
name: routing
questions:
  - question: Which region sold the most?
    ground_truth:
      ground_truth_subagents: ["{{ agent('sales') }}", "{{ tool('partner_agent') }}"]
      ground_truth_output: The reply names a region.
  - question: Which supplier is late?
    ground_truth:
      ground_truth_subagents: []
      ground_truth_output: The reply declines.
"""
CONFIG = """
name: ci
config:
  agent_version: committed
dataset:
  name_template: "EVAL_{{ agent | upper }}_{{ sha7 }}"
  source_table_template: "EVAL_SRC_{{ agent | upper }}_{{ sha7 }}"
metrics:
  routing:
    - name: routing_accuracy
      gate: true
      threshold: {min: 0.9}
  system:
    - name: answer_correctness
      version: v3
"""


def _profile_project(tmp_path: Path, *, dataset: str = DATASET, config: str = CONFIG, evals: str | None = None) -> None:
    block = (
        evals
        or """
evals:
  dataset: "{{ eval_dataset('routing') }}"
  config: "{{ eval_config('ci') }}"
"""
    )
    write(
        tmp_path / PROFILE / "cowork_profile.yml",
        """
name: concierge
spec:
  subagents:
    - agent: "{{ agent('sales') }}"
    - agent: "{{ tool('partner_agent') }}"
"""
        + block,
    )
    write(tmp_path / PROFILE / "evals" / "dataset.yml", dataset)
    write(tmp_path / PROFILE / "evals" / "config.yml", config)


def _catalog(tmp_path: Path) -> EvalCatalog:
    profiles, diagnostics = load_cowork_profiles(tmp_path)
    return load_eval_catalog(tmp_path, (), initial_diagnostics=diagnostics, cowork_profiles=profiles)


def test_a_profile_names_its_eval_files_as_an_agent_does(tmp_path: Path) -> None:
    _profile_project(tmp_path)
    profiles, diagnostics = load_cowork_profiles(tmp_path)
    assert not diagnostics.has_errors
    [profile] = profiles
    assert profile.evals is not None
    assert (profile.evals.dataset, profile.evals.config, profile.evals.folder) == ("routing", "ci", f"{PROFILE}/evals")


def test_a_profile_eval_loads_its_expected_subagents_and_routing_metric(tmp_path: Path) -> None:
    _profile_project(tmp_path)
    catalog = _catalog(tmp_path)
    assert not catalog.diagnostics.has_errors, [item.message for item in catalog.diagnostics]
    [value] = catalog.evals
    assert value.key == "eval:cowork_profile:concierge"
    first, second = (row.ground_truth for row in value.dataset.questions)
    assert first is not None and second is not None
    assert [item.reference for item in first.subagents or ()] == ["agent:sales", "tool:partner_agent"]
    assert second.subagents == ()
    assert "ground_truth_subagents" not in first.extra
    [metric] = value.config.routing_metrics
    assert (metric.name, metric.gate, metric.threshold) == ("routing_accuracy", True, metric.threshold)
    assert metric.threshold is not None and metric.threshold.min == 0.9


def test_a_profile_eval_file_it_does_not_name_is_reported_under_the_profile_eval_key(tmp_path: Path) -> None:
    _profile_project(
        tmp_path,
        evals="""
evals:
  dataset: "{{ eval_dataset('other') }}"
  config: "{{ eval_config('ci') }}"
""",
    )
    catalog = _catalog(tmp_path)
    [missing] = [item for item in catalog.diagnostics if item.code == "SST-REF048"]
    assert missing.subject == "eval:cowork_profile:concierge"
    assert catalog.evals == ()


def test_wrong_expected_subagents_are_reported_and_left_out(tmp_path: Path) -> None:
    dataset = """
name: routing
questions:
  - question: Which region sold the most?
    ground_truth:
      ground_truth_subagents: "{{ agent('sales') }}"
  - question: Which supplier is late?
    ground_truth:
      ground_truth_subagents: [3, "sales", "{{ agent('sales') }}"]
"""
    _profile_project(tmp_path, dataset=dataset)
    catalog = _catalog(tmp_path)
    field = "questions[{}].ground_truth.ground_truth_subagents"
    found = [(item.code, item.context.get("field")) for item in catalog.diagnostics]
    assert found[:3] == [
        ("SST-PRS003", field.format(0)),
        ("SST-PRS018", field.format(1)),
        ("SST-REF050", field.format(1)),
    ]
    [value] = catalog.evals
    first, second = (row.ground_truth for row in value.dataset.questions)
    assert first is not None and first.subagents is None
    assert second is not None and [item.reference for item in second.subagents or ()] == ["agent:sales"]


def test_wrong_routing_metrics_are_reported(tmp_path: Path) -> None:
    config = CONFIG.replace(
        """  routing:
    - name: routing_accuracy
      gate: true
      threshold: {min: 0.9}
""",
        """  routing:
    - not a mapping
    - gate: yes please
    - name: route_precision
""",
    )
    _profile_project(tmp_path, config=config)
    catalog = _catalog(tmp_path)
    found = [(item.code, item.context.get("field")) for item in catalog.diagnostics if item.code.startswith("SST-PRS")]
    assert ("SST-PRS018", "metrics.routing") in found
    assert ("SST-PRS002", "metrics.routing[1].name") in found
    assert ("SST-PRS019", "gate") in found
    assert ("SST-PRS013", "metrics.routing[2].name") in found
    [value] = catalog.evals
    assert [metric.name for metric in value.config.routing_metrics] == [None, "route_precision"]


def test_a_routing_block_that_is_not_a_list_is_reported(tmp_path: Path) -> None:
    _profile_project(
        tmp_path,
        config=CONFIG.replace(
            "  routing:\n    - name: routing_accuracy", "  routing: {}\n  x:\n    - name: routing_accuracy"
        ),
    )
    found = [(item.code, item.context.get("field")) for item in _catalog(tmp_path).diagnostics]
    assert ("SST-PRS003", "metrics.routing") in found


def test_an_eval_route_may_name_a_profile_folder(tmp_path: Path) -> None:
    _profile_project(tmp_path)
    (tmp_path / "agents" / "sales").mkdir(parents=True)
    config = {"evals": {"concierge": {"+eval_tier": "report"}, "sales": {}, "nowhere": {"deeper": {}}}}
    found = routed_diagnostics(config, tmp_path, "eval", also_under=("cowork_profile",))
    assert [item.subject for item in found] == ["config_route:nowhere"]
    assert [item.subject for item in routed_diagnostics(config, tmp_path, "eval")] == [
        "config_route:concierge",
        "config_route:nowhere",
    ]


def test_the_project_source_loads_the_profiles_it_is_not_given(tmp_path: Path) -> None:
    project = with_profile_eval(project_copy(tmp_path))
    source = YamlProjectSource(project_paths(project), target_name="dev", manifest_path=DBT_MANIFEST, invoke_dbt=False)
    keys = [item.key for item in source.load_evals().evals]
    assert keys == ["eval:jaffle_analytics_agent", "eval:cowork_profile:jaffle_concierge"]
    assert [item.key for item in source.load_evals(cowork_profiles=()).evals] == ["eval:jaffle_analytics_agent"]
