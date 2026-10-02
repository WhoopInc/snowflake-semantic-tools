"""`sst init`: scaffold SST into a dbt project, never overwriting a file that exists.

The configuration it writes sets every policy key explicitly, each at the value the reference
documents, so a scaffolded project never relies on a default it cannot see. `--check-only`
reports what is in place and creates nothing.
"""

from __future__ import annotations

import sys
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.cli.exit_codes import CONFIG, ERROR, OK
from snowflake_semantic_tools.cli.globals import GlobalOptions
from snowflake_semantic_tools.cli.runner import CommandResult, ConfigNeed, command_body, write_text
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.config_schema import CONFIG_FILE, CONFIG_KEYS

_DEFAULT_SEMANTIC_DIR = "semantic_models"


def config_template(semantic_models_dir: str) -> str:
    """Return the configuration `sst init` writes, every policy key at its documented default."""

    def default(path: str) -> str:
        return CONFIG_KEYS[path].default or ""

    return (
        "project:\n"
        f"  semantic_models_dir: {semantic_models_dir}\n"
        "\n"
        "validation:\n"
        f"  strict: {default('validation.strict')}\n"
        f"  snowflake_syntax_check: {default('validation.snowflake_syntax_check')}\n"
        "\n"
        "enrichment:\n"
        f"  distinct_limit: {default('enrichment.distinct_limit')}\n"
        f"  sample_values_display_limit: {default('enrichment.sample_values_display_limit')}\n"
        f"  synonym_model: {default('enrichment.synonym_model')}\n"
        f"  synonym_max_count: {default('enrichment.synonym_max_count')}\n"
        f"  allow_sample_value_collection: {default('enrichment.allow_sample_value_collection')}\n"
        "\n"
        "apply:\n"
        f"  fail_fast: {default('apply.fail_fast')}\n"
        "\n"
        "state:\n"
        '  +database: "{{ target.database }}"\n'
        '  +schema: "{{ target.schema }}"\n'
        f"  +table: {default('state.+table')}\n"
    )


@click.command()
@click.option("--skip-prompts", is_flag=True)
@click.option("--check-only", is_flag=True)
@command_body("init", config=ConfigNeed.OPTIONAL)
def init(paths: ProjectPaths, skip_prompts: bool, check_only: bool, options: GlobalOptions) -> CommandResult:
    """Scaffold SST into an existing dbt project without overwriting a file.

    Writes the configuration file and the semantic-model directories it names. With
    --check-only nothing is written, and the command exits 1 unless the setup is complete.
    Exit 4 when the directory is not a dbt project.
    """
    project_dir = paths.project_dir
    if not (project_dir / "dbt_project.yml").is_file():
        refusal = D("SST-CFG046", subject="config:project", key="sst init")
        return CommandResult(
            CONFIG, DiagnosticBag((refusal,)), {"created": [], "skipped": [], "status": "not a dbt project"}
        )
    config = paths.config_file or project_dir / CONFIG_FILE
    views = Path(_DEFAULT_SEMANTIC_DIR) / "semantic_views"
    wanted = (config, project_dir / views)
    present = [str(path.relative_to(project_dir)) for path in wanted if path.exists()]
    missing = [str(path.relative_to(project_dir)) for path in wanted if not path.exists()]
    if check_only:
        status = "incomplete" if missing else "complete"
        data = {"created": [], "skipped": present, "missing": missing, "status": status}
        return CommandResult(ERROR if missing else OK, data=data, human=lambda: click.echo(f"setup {status}"))
    created: list[str] = []
    semantic_dir = _DEFAULT_SEMANTIC_DIR
    if not config.exists():
        if not skip_prompts and options.output != "json" and sys.stdin.isatty():
            semantic_dir = click.prompt("Semantic models directory", default=_DEFAULT_SEMANTIC_DIR)
        write_text(config, config_template(semantic_dir))
        created.append(config.name)
    directory = project_dir / semantic_dir / "semantic_views"
    if not directory.is_dir():
        directory.mkdir(parents=True)
        created.append(str(directory.relative_to(project_dir)))
    skipped = [item for item in present if item not in created]
    data = {"created": created, "skipped": skipped, "status": "scaffolded"}
    return CommandResult(
        data=data, human=lambda: click.echo(f"initialized SST project; created {len(created)} item(s)")
    )
