"""
SST Init Wizard

Interactive setup wizard for initializing SST in a dbt project.
Guides users through configuration, profile setup, and example file creation.
"""

import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import questionary
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.rule import Rule
from rich.table import Table

from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger(__name__)
console = Console()


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class DbtProjectInfo:
    """Information extracted from dbt_project.yml."""

    name: str
    profile_name: str
    model_paths: List[str] = field(default_factory=lambda: ["models"])
    project_path: Path = field(default_factory=Path.cwd)


@dataclass
class ProfileInfo:
    """Information about an existing dbt profile."""

    name: str
    targets: List[str]
    default_target: str
    path: Path


@dataclass
class WizardConfig:
    """Configuration gathered during the wizard."""

    semantic_models_dir: str = "snowflake_semantic_models"
    dbt_models_dir: str = "models"
    create_examples: bool = True
    test_connection: bool = False


# =============================================================================
# Init Wizard
# =============================================================================


class InitWizard:
    """
    Interactive setup wizard for SST.

    Guides users through:
    - Detecting/requiring dbt project
    - Setting up Snowflake profile
    - Creating sst_config.yaml
    - Creating semantic models directory structure
    - Generating example files
    """

    TEMPLATES_DIR = Path(__file__).parent.parent / "templates"

    def __init__(self, project_dir: Optional[Path] = None, skip_prompts: bool = False):
        """
        Initialize the wizard.

        Args:
            project_dir: Path to dbt project. Defaults to current directory.
            skip_prompts: If True, use defaults without prompting.
        """
        self.project_dir = Path(project_dir) if project_dir else Path.cwd()
        self.skip_prompts = skip_prompts
        self.dbt_project: Optional[DbtProjectInfo] = None
        self.profile_info: Optional[ProfileInfo] = None

    def run(self) -> bool:
        """
        Run the complete initialization wizard.

        Returns:
            True if setup completed successfully, False otherwise.
        """
        try:
            # Welcome banner
            self._show_welcome()

            # Step 1: Detect dbt project (required)
            self.dbt_project = self.detect_dbt_project()
            if not self.dbt_project:
                return False

            console.print(f"[green]✓[/green] Detected dbt project: [cyan]{self.dbt_project.name}[/cyan]")

            # Step 2: Check/create Snowflake profile
            self.profile_info = self.detect_profile(self.dbt_project.profile_name)
            if self.profile_info:
                targets_str = ", ".join(self.profile_info.targets)
                console.print(
                    f"[green]✓[/green] Found profile: [cyan]{self.profile_info.name}[/cyan] "
                    f"(targets: {targets_str})"
                )
            else:
                console.print(
                    f"[yellow]✗[/yellow] No Snowflake profile found for "
                    f"'[cyan]{self.dbt_project.profile_name}[/cyan]'"
                )
                console.print()
                if not self._create_profile_interactive(self.dbt_project.profile_name):
                    return False

            # Step 3: Check for existing sst_config.yaml
            existing_config = self.detect_existing_config()
            if existing_config and not self.skip_prompts:
                if not self._handle_existing_config(existing_config):
                    return False

            # Step 4: Gather configuration
            config = self._prompt_config()
            if config is None:
                console.print("[yellow]Setup cancelled.[/yellow]")
                return False

            # Step 5: Show confirmation
            if not self.skip_prompts:
                if not self._confirm_creation(config):
                    console.print("[yellow]Setup cancelled.[/yellow]")
                    return False

            # Step 6: Create files
            self._create_files(config)

            # Step 7: Test connection (optional)
            if config.test_connection:
                self._test_snowflake_connection()

            # Step 8: Show next steps
            self._show_next_steps()

            return True

        except KeyboardInterrupt:
            console.print("\n[yellow]Setup cancelled.[/yellow]")
            return False
        except Exception as e:
            logger.exception("Init wizard failed")
            console.print(f"\n[red]Error:[/red] {e}")
            return False

    def _show_welcome(self) -> None:
        """Display welcome banner."""
        console.print()
        console.print(
            Panel(
                "[bold cyan]Welcome to Snowflake Semantic Tools![/bold cyan]\n\n"
                "This wizard will help you set up SST in your dbt project.",
                border_style="cyan",
            )
        )
        console.print()

    # -------------------------------------------------------------------------
    # Detection Methods
    # -------------------------------------------------------------------------

    def detect_dbt_project(self) -> Optional[DbtProjectInfo]:
        """
        Detect dbt project in current directory.

        Returns:
            DbtProjectInfo if found, None otherwise.
        """
        dbt_project_file = self.project_dir / "dbt_project.yml"

        if not dbt_project_file.exists():
            console.print("[red]✗[/red] No dbt project detected.")
            console.print()
            console.print("  SST requires a dbt project to function. Please run this command")
            console.print("  from inside a dbt project directory (must contain dbt_project.yml).")
            console.print()
            console.print("  To create a new dbt project:")
            console.print("    [cyan]dbt init my_project[/cyan]")
            console.print("    [cyan]cd my_project[/cyan]")
            console.print("    [cyan]sst init[/cyan]")
            console.print()
            return None

        try:
            with open(dbt_project_file, "r", encoding="utf-8") as f:
                project_config = yaml.safe_load(f) or {}

            name = project_config.get("name", "unknown")
            profile_name = project_config.get("profile", name)
            model_paths = project_config.get("model-paths", ["models"])

            return DbtProjectInfo(
                name=name,
                profile_name=profile_name,
                model_paths=model_paths,
                project_path=self.project_dir,
            )

        except yaml.YAMLError as e:
            console.print(f"[red]✗[/red] Error reading dbt_project.yml: {e}")
            return None

    def detect_profile(self, profile_name: str) -> Optional[ProfileInfo]:
        """
        Detect if the required profile exists in profiles.yml.

        Args:
            profile_name: Name of the profile to find.

        Returns:
            ProfileInfo if found, None otherwise.
        """
        # Check standard locations
        profiles_paths = [
            self.project_dir / "profiles.yml",
            Path.home() / ".dbt" / "profiles.yml",
        ]

        for profiles_path in profiles_paths:
            if not profiles_path.exists():
                continue

            try:
                with open(profiles_path, "r", encoding="utf-8") as f:
                    profiles = yaml.safe_load(f) or {}

                if profile_name in profiles:
                    profile = profiles[profile_name]
                    outputs = profile.get("outputs", {})
                    targets = list(outputs.keys())
                    default_target = profile.get("target", targets[0] if targets else "dev")

                    # Verify it's a Snowflake profile
                    if targets:
                        first_output = outputs.get(targets[0], {})
                        if first_output.get("type") != "snowflake":
                            continue

                    return ProfileInfo(
                        name=profile_name,
                        targets=targets,
                        default_target=default_target,
                        path=profiles_path,
                    )

            except yaml.YAMLError:
                continue

        return None

    def detect_existing_config(self) -> Optional[Path]:
        """
        Check if sst_config.yaml already exists.

        Returns:
            Path to existing config if found, None otherwise.
        """
        for ext in ["yaml", "yml"]:
            config_path = self.project_dir / f"sst_config.{ext}"
            if config_path.exists():
                return config_path
        return None

    # -------------------------------------------------------------------------
    # Profile Creation
    # -------------------------------------------------------------------------

    def _create_profile_interactive(self, profile_name: str) -> bool:
        """
        Interactively create a Snowflake profile.

        Args:
            profile_name: Name for the new profile.

        Returns:
            True if profile created successfully.
        """
        console.print("  Let's set up your Snowflake connection.")
        console.print()

        if self.skip_prompts:
            console.print("[yellow]Cannot create profile in --skip-prompts mode.[/yellow]")
            console.print("Please create ~/.dbt/profiles.yml manually.")
            return False

        # Gather credentials
        account = self._prompt_with_validation(
            "Snowflake account (e.g., abc12345.us-east-1)",
            validator=self._validate_account,
            default=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
        )

        user = self._prompt_text("Username", default=os.environ.get("SNOWFLAKE_USER", ""))

        auth_method = questionary.select(
            "Authentication method:",
            choices=[
                questionary.Choice("SSO (Browser) - recommended", value="sso"),
                questionary.Choice("Password", value="password"),
                questionary.Choice("Key Pair (RSA)", value="keypair"),
            ],
            default="sso",
        ).ask()

        if auth_method is None:
            return False

        private_key_path = ""
        if auth_method == "keypair":
            private_key_path = self._prompt_text(
                "Private key path", default=str(Path.home() / ".ssh" / "snowflake_key.p8")
            )

        role = self._prompt_text("Role", default=os.environ.get("SNOWFLAKE_ROLE", ""))
        warehouse = self._prompt_text("Warehouse", default=os.environ.get("SNOWFLAKE_WAREHOUSE", ""))
        database = self._prompt_text("Database", default=os.environ.get("SNOWFLAKE_DATABASE", ""))
        schema = self._prompt_text("Schema", default="DEV")

        # Create profiles.yml
        profiles_dir = Path.home() / ".dbt"
        profiles_dir.mkdir(parents=True, exist_ok=True)
        profiles_path = profiles_dir / "profiles.yml"

        # Load existing profiles if any
        existing_profiles: Dict[str, Any] = {}
        if profiles_path.exists():
            try:
                with open(profiles_path, "r", encoding="utf-8") as f:
                    existing_profiles = yaml.safe_load(f) or {}
            except yaml.YAMLError:
                existing_profiles = {}

        # Build new profile
        output_config: Dict[str, Any] = {
            "type": "snowflake",
            "account": account,
            "user": user,
            "role": role,
            "warehouse": warehouse,
            "database": database,
            "schema": schema,
            "threads": 4,
        }

        if auth_method == "sso":
            output_config["authenticator"] = "externalbrowser"
        elif auth_method == "password":
            output_config["password"] = "{{ env_var('SNOWFLAKE_PASSWORD') }}"
        elif auth_method == "keypair":
            output_config["private_key_path"] = private_key_path
            output_config["private_key_passphrase"] = "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', '') }}"

        new_profile = {
            "target": "dev",
            "outputs": {"dev": output_config},
        }

        existing_profiles[profile_name] = new_profile

        # Write profiles.yml
        with open(profiles_path, "w", encoding="utf-8") as f:
            yaml.dump(existing_profiles, f, default_flow_style=False, sort_keys=False)

        console.print()
        console.print(f"[green]✓[/green] Created [cyan]{profiles_path}[/cyan]")

        # Show env var instructions if needed
        if auth_method == "password":
            console.print()
            console.print("  [yellow]Set your password environment variable:[/yellow]")
            console.print("    [cyan]export SNOWFLAKE_PASSWORD='your_password'[/cyan]")
        elif auth_method == "keypair":
            console.print()
            console.print("  [yellow]If your key has a passphrase, set:[/yellow]")
            console.print("    [cyan]export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE='your_passphrase'[/cyan]")

        console.print()

        # Update profile_info
        self.profile_info = ProfileInfo(
            name=profile_name,
            targets=["dev"],
            default_target="dev",
            path=profiles_path,
        )

        return True

    # -------------------------------------------------------------------------
    # Configuration
    # -------------------------------------------------------------------------

    def _prompt_config(self) -> Optional[WizardConfig]:
        """Gather configuration from user or use defaults."""
        config = WizardConfig()

        if self.skip_prompts:
            # Use defaults, detect models dir from dbt_project.yml
            if self.dbt_project and self.dbt_project.model_paths:
                config.dbt_models_dir = self.dbt_project.model_paths[0]
            return config

        console.print()

        # Semantic models directory
        semantic_dir = questionary.select(
            "Where should SST store semantic models?",
            choices=[
                questionary.Choice("snowflake_semantic_models (recommended)", value="snowflake_semantic_models"),
                questionary.Choice("semantic_models", value="semantic_models"),
                questionary.Choice("Custom path...", value="_custom"),
            ],
            default="snowflake_semantic_models",
        ).ask()

        if semantic_dir is None:
            return None  # User cancelled

        if semantic_dir == "_custom":
            semantic_dir = self._prompt_text("Custom path", default="snowflake_semantic_models")
            if not semantic_dir:
                return None  # User cancelled

        config.semantic_models_dir = semantic_dir

        # dbt models directory (auto-detect from dbt_project.yml)
        default_models_dir = "models"
        if self.dbt_project and self.dbt_project.model_paths:
            default_models_dir = self.dbt_project.model_paths[0]
        config.dbt_models_dir = default_models_dir

        # Create examples?
        create_examples = questionary.select(
            "Create example semantic models?",
            choices=[
                questionary.Choice("Yes, show me examples", value=True),
                questionary.Choice("No, I'll create my own", value=False),
            ],
            default=True,
        ).ask()

        if create_examples is None:
            return None  # User cancelled
        config.create_examples = create_examples

        # Test connection?
        if self.profile_info:
            test_conn = questionary.select(
                "Test Snowflake connection?",
                choices=[
                    questionary.Choice("Yes", value=True),
                    questionary.Choice("No", value=False),
                ],
                default=True,
            ).ask()

            if test_conn is None:
                return None  # User cancelled
            config.test_connection = test_conn

        return config

    def _handle_existing_config(self, config_path: Path) -> bool:
        """Handle existing sst_config.yaml."""
        console.print(f"[yellow]![/yellow] Found existing config: [cyan]{config_path}[/cyan]")

        action = questionary.select(
            "What would you like to do?",
            choices=[
                questionary.Choice("Overwrite with new config", value="overwrite"),
                questionary.Choice("Keep existing and skip", value="skip"),
            ],
            default="skip",
        ).ask()

        if action == "skip":
            console.print("[dim]Keeping existing configuration.[/dim]")
            return True
        elif action == "overwrite":
            return True

        return False

    def _confirm_creation(self, config: WizardConfig) -> bool:
        """Show summary and confirm before creating files."""
        console.print()
        console.print(Rule("Summary"))
        console.print()
        console.print("The following will be created:")
        console.print()

        # Check what needs to be created
        items = []

        sst_config_path = self.project_dir / "sst_config.yaml"
        if not sst_config_path.exists():
            items.append(("sst_config.yaml", "new"))
        else:
            items.append(("sst_config.yaml", "overwrite"))

        semantic_dir = self.project_dir / config.semantic_models_dir
        if not semantic_dir.exists():
            items.append((f"{config.semantic_models_dir}/", "new directory"))
            if config.create_examples:
                items.append((f"  ├── metrics/_examples.yml", ""))
                items.append((f"  ├── relationships/_examples.yml", ""))
                items.append((f"  ├── filters/_examples.yml", ""))
                items.append((f"  ├── verified_queries/_examples.yml", ""))
                items.append((f"  ├── custom_instructions/_examples.yml", ""))
                items.append((f"  ├── semantic_views.yml", ""))
                items.append((f"  └── README.md", ""))

        for item, status in items:
            if status:
                console.print(f"  • {item} [dim]({status})[/dim]")
            else:
                console.print(f"  {item}")

        console.print()

        confirm = questionary.confirm("Proceed?", default=True).ask()
        return confirm if confirm is not None else False

    # -------------------------------------------------------------------------
    # File Creation
    # -------------------------------------------------------------------------

    def _create_files(self, config: WizardConfig) -> None:
        """Create all configuration and example files."""
        console.print()

        # Create sst_config.yaml
        self._create_sst_config(config)

        # Create semantic models directory structure
        self._create_directories(config)

        # Create example files
        if config.create_examples:
            self._create_example_files(config)

    def _create_sst_config(self, config: WizardConfig) -> None:
        """Create sst_config.yaml from template."""
        template_path = self.TEMPLATES_DIR / "sst_config.yaml.j2"
        output_path = self.project_dir / "sst_config.yaml"

        if template_path.exists():
            with open(template_path, "r", encoding="utf-8") as f:
                template = f.read()

            content = template.replace("{{ semantic_models_dir }}", config.semantic_models_dir)
            content = content.replace("{{ dbt_models_dir }}", config.dbt_models_dir)

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(content)
        else:
            # Fallback: create basic config
            config_content = {
                "project": {
                    "semantic_models_dir": config.semantic_models_dir,
                    "dbt_models_dir": config.dbt_models_dir,
                },
                "validation": {"strict": False, "exclude_dirs": ["_intermediate", "staging"]},
                "enrichment": {
                    "distinct_limit": 25,
                    "sample_values_display_limit": 10,
                    "synonym_model": "mistral-large2",
                    "synonym_max_count": 4,
                },
            }
            with open(output_path, "w", encoding="utf-8") as f:
                yaml.dump(config_content, f, default_flow_style=False, sort_keys=False)

        console.print(f"[green]✓[/green] Created [cyan]sst_config.yaml[/cyan]")

    def _create_directories(self, config: WizardConfig) -> None:
        """Create semantic models directory structure."""
        base_dir = self.project_dir / config.semantic_models_dir
        subdirs = [
            "metrics",
            "relationships",
            "filters",
            "verified_queries",
            "custom_instructions",
        ]

        for subdir in subdirs:
            dir_path = base_dir / subdir
            dir_path.mkdir(parents=True, exist_ok=True)

        console.print(f"[green]✓[/green] Created [cyan]{config.semantic_models_dir}/[/cyan]")

    def _create_example_files(self, config: WizardConfig) -> None:
        """Create example files in semantic models directory."""
        base_dir = self.project_dir / config.semantic_models_dir
        examples_dir = self.TEMPLATES_DIR / "examples"

        # Map of template files to destination
        file_mapping = {
            "metrics_examples.yml": "metrics/_examples.yml",
            "relationships_examples.yml": "relationships/_examples.yml",
            "filters_examples.yml": "filters/_examples.yml",
            "verified_queries_examples.yml": "verified_queries/_examples.yml",
            "custom_instructions_examples.yml": "custom_instructions/_examples.yml",
            "semantic_views_examples.yml": "semantic_views.yml",
        }

        for src_name, dest_name in file_mapping.items():
            src_path = examples_dir / src_name
            dest_path = base_dir / dest_name

            if src_path.exists():
                shutil.copy(src_path, dest_path)

        # Create README
        readme_src = self.TEMPLATES_DIR / "semantic_models_readme.md"
        readme_dest = base_dir / "README.md"
        if readme_src.exists():
            shutil.copy(readme_src, readme_dest)

        console.print("[green]✓[/green] Created example files")

    # -------------------------------------------------------------------------
    # Connection Test
    # -------------------------------------------------------------------------

    def _test_snowflake_connection(self) -> None:
        """Test Snowflake connection."""
        console.print()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            progress.add_task("Testing Snowflake connection...", total=None)

            try:
                from snowflake_semantic_tools.interfaces.cli.utils import snowflake_session

                with snowflake_session() as client:
                    result = client.execute_query("SELECT CURRENT_USER(), CURRENT_ROLE()")

                    if not result.empty:
                        user = result.iloc[0, 0]
                        role = result.iloc[0, 1]

                console.print()
                console.print("[green]✓[/green] Connection successful!")
                console.print(f"  Connected as: [cyan]{user}[/cyan]")
                console.print(f"  Current role: [cyan]{role}[/cyan]")

            except Exception as e:
                console.print()
                console.print(f"[yellow]![/yellow] Connection test failed: {e}")
                console.print("  You can test the connection later with: [cyan]sst debug --test-connection[/cyan]")

    # -------------------------------------------------------------------------
    # Next Steps
    # -------------------------------------------------------------------------

    def _show_next_steps(self) -> None:
        """Display next steps after setup."""
        console.print()
        console.print(Rule("Setup Complete!", style="green"))
        console.print()
        console.print("Next steps:")
        console.print("  1. [cyan]sst debug[/cyan]              # Verify configuration")
        console.print("  2. [cyan]sst enrich models/[/cyan]     # Add metadata to dbt models")
        console.print("  3. [cyan]sst validate[/cyan]           # Check semantic models")
        console.print()
        console.print(
            "Documentation: [link=https://github.com/WhoopInc/snowflake-semantic-tools]"
            "https://github.com/WhoopInc/snowflake-semantic-tools[/link]"
        )
        console.print()

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _prompt_text(self, message: str, default: str = "") -> str:
        """Prompt for text input."""
        result = questionary.text(message, default=default).ask()
        return result if result is not None else default

    def _prompt_with_validation(
        self,
        message: str,
        validator: callable,
        default: str = "",
    ) -> str:
        """Prompt with validation."""
        while True:
            result = questionary.text(message, default=default).ask()
            if result is None:
                return default

            error = validator(result)
            if error is None:
                return result
            console.print(f"[red]{error}[/red]")

    def _validate_account(self, value: str) -> Optional[str]:
        """Validate Snowflake account format."""
        if not value:
            return "Account is required"
        if "." not in value and "-" not in value:
            return "Account should include region (e.g., abc12345.us-east-1)"
        return None
