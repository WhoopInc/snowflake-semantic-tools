"""What the per-code PRS tests share: an eval, an agent, a dataset or a skill tree, read for one code."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.adapters.yaml.evals.dataset import parse_dataset
from snowflake_semantic_tools.adapters.yaml.skills import load_skill_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.eval import CustomEvalMetric, EvalCatalog, ResolvedEval
from snowflake_semantic_tools.domain.validate.eval import validate_eval_catalog
from tests.helpers.eval_builders import resolved_eval
from tests.helpers.file_trees import write_tree
from tests.helpers.seam_projects import parsed

VALUE = resolved_eval()


def eval_catalog_findings(
    code: str, value: ResolvedEval = VALUE, metrics: tuple[CustomEvalMetric, ...] | None = None
) -> list[Diagnostic]:
    """The diagnostics of `code` from validating a catalog of `value` and its custom metrics."""
    custom = value.custom_metrics if metrics is None else metrics
    catalog = EvalCatalog((value,), custom)
    return [item for item in validate_eval_catalog(catalog, allowed_models=("claude-sonnet-4-6",)) if item.code == code]


def agent_spec_findings(tmp_path: Path, code: str, spec: str) -> list[Diagnostic]:
    """The diagnostics of `code` from loading agent `sales` whose `spec:` block is `spec`."""
    folder = tmp_path / "agents" / "sales"
    folder.mkdir(parents=True)
    (folder / "agent.yml").write_text(f"name: sales\nspec:\n{spec}", encoding="utf-8")
    return [item for item in load_agents(tmp_path)[1] if item.code == code]


FILE = "agents/sales/evals/dataset.yml"


def dataset_findings(code: str, rows: str) -> list[Diagnostic]:
    """The diagnostics of `code` from parsing an eval dataset for agent `sales` with `rows`."""
    diagnostics: list[Diagnostic] = []
    parse_dataset((FILE, parsed(f"agent: sales\nquestions:\n{rows}", FILE)), diagnostics)
    return [item for item in diagnostics if item.code == code]


def skill_tree_findings(tmp_path: Path, code: str, files: dict[str, str]) -> list[Diagnostic]:
    """The diagnostics of `code` from loading the skill catalog written from `files`."""
    write_tree(tmp_path, files)
    catalog = load_skill_catalog(tmp_path, skills_dir="skills", plugins_dir="plugins")
    return [item for item in catalog.diagnostics if item.code == code]
