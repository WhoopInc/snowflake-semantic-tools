"""
Init Command

CLI command for initializing SST in a dbt project.
Provides an interactive wizard to set up configuration, profiles, and examples.
"""

import sys
from pathlib import Path

import click


@click.command("init")
@click.option(
    "--skip-prompts",
    is_flag=True,
    help="Use defaults without prompting (non-interactive mode)",
)
@click.option(
    "--check-only",
    is_flag=True,
    help="Check current setup status without creating files",
)
def init(skip_prompts: bool, check_only: bool) -> None:
    """
    Initialize SST in a dbt project.

    Guides you through setting up Snowflake Semantic Tools with an interactive
    wizard that:

    \b
    - Detects your dbt project configuration
    - Sets up Snowflake connection (profiles.yml)
    - Creates sst_config.yaml
    - Creates semantic models directory with examples

    \b
    Examples:
        # Interactive setup
        sst init

        # Non-interactive with defaults
        sst init --skip-prompts

        # Check current setup status
        sst init --check-only
    """
    from rich.console import Console

    from snowflake_semantic_tools.services.init_wizard import InitWizard

    console = Console()

    if check_only:
        _check_setup_status(console)
        return

    wizard = InitWizard(
        project_dir=Path.cwd(),
        skip_prompts=skip_prompts,
    )

    success = wizard.run()

    if not success:
        sys.exit(1)


def _check_setup_status(console) -> None:
    """Check and display current setup status."""
    from pathlib import Path

    import yaml

    console.print()
    console.print("[bold]SST Setup Status[/bold]")
    console.print()

    project_dir = Path.cwd()
    all_good = True

    # Check dbt_project.yml
    dbt_project_file = project_dir / "dbt_project.yml"
    if dbt_project_file.exists():
        try:
            with open(dbt_project_file, "r") as f:
                project = yaml.safe_load(f)
            project_name = project.get("name", "unknown")
            profile_name = project.get("profile", project_name)
            console.print(f"[green]✓[/green] dbt project: [cyan]{project_name}[/cyan]")
        except Exception:
            console.print("[yellow]![/yellow] dbt_project.yml exists but could not be parsed")
            all_good = False
    else:
        console.print("[red]✗[/red] No dbt_project.yml found")
        all_good = False
        profile_name = None

    # Check profiles.yml
    if profile_name:
        profiles_paths = [
            project_dir / "profiles.yml",
            Path.home() / ".dbt" / "profiles.yml",
        ]
        profile_found = False
        for path in profiles_paths:
            if path.exists():
                try:
                    with open(path, "r") as f:
                        profiles = yaml.safe_load(f)
                    if profile_name in profiles:
                        targets = list(profiles[profile_name].get("outputs", {}).keys())
                        console.print(
                            f"[green]✓[/green] Profile '{profile_name}' found " f"(targets: {', '.join(targets)})"
                        )
                        profile_found = True
                        break
                except Exception:
                    continue

        if not profile_found:
            console.print(f"[red]✗[/red] Profile '{profile_name}' not found in profiles.yml")
            all_good = False

    # Check sst_config.yaml
    sst_config_found = False
    for ext in ["yaml", "yml"]:
        config_path = project_dir / f"sst_config.{ext}"
        if config_path.exists():
            console.print(f"[green]✓[/green] sst_config.{ext} exists")
            sst_config_found = True
            break

    if not sst_config_found:
        console.print("[red]✗[/red] No sst_config.yaml found")
        all_good = False

    # Check semantic models directory
    from snowflake_semantic_tools.shared.config import get_config

    try:
        config = get_config()
        semantic_dir = config.get("project.semantic_models_dir", "snowflake_semantic_models")
        semantic_path = project_dir / semantic_dir
        if semantic_path.exists():
            # Count files
            yml_files = list(semantic_path.rglob("*.yml")) + list(semantic_path.rglob("*.yaml"))
            console.print(
                f"[green]✓[/green] Semantic models directory: [cyan]{semantic_dir}[/cyan] " f"({len(yml_files)} files)"
            )
        else:
            console.print(f"[yellow]![/yellow] Semantic models directory not found: {semantic_dir}")
    except Exception:
        pass

    console.print()

    if all_good:
        console.print("[green]Setup complete![/green] Run [cyan]sst debug[/cyan] to verify.")
    else:
        console.print("Run [cyan]sst init[/cyan] to complete setup.")

    console.print()
