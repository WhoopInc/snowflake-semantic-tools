"""
Debug Command

CLI command for displaying configuration and testing Snowflake connectivity.
Helps users verify their dbt profile setup is correct.
"""

import traceback

import click

from snowflake_semantic_tools._version import __version__


@click.command()
@click.option("--target", "-t", "dbt_target", help="dbt target from profiles.yml (default: uses profile's default)")
@click.option("--test-connection", is_flag=True, help="Test Snowflake connection")
@click.option("--verbose", "-v", is_flag=True, help="Show additional details")
def debug(dbt_target, test_connection, verbose):
    """
    Show configuration and optionally test Snowflake connection.

    Displays the current dbt profile configuration that SST will use,
    helping verify your setup is correct before running other commands.

    \b
    Examples:
        # Show current configuration
        sst debug

        # Test the Snowflake connection
        sst debug --test-connection

        # Use a specific target
        sst debug --target prod
    """
    click.echo()
    click.echo(click.style(f"SST Debug", bold=True) + f" (v{__version__})")
    click.echo()

    # Try to load the dbt profile configuration
    try:
        from snowflake_semantic_tools.infrastructure.dbt.profile_parser import DbtProfileParser

        parser = DbtProfileParser()

        # Get profile name from dbt_project.yml
        try:
            profile_name = parser.get_profile_name()
            project_file = parser.project_dir / "dbt_project.yml"
        except Exception as e:
            _error(f"Could not read dbt_project.yml: {e}")
            raise click.Abort()

        # Find profiles.yml
        profiles_path = parser.find_profiles_yml()
        if not profiles_path:
            _error("No profiles.yml found")
            click.echo("  Searched locations:")
            for path in parser.get_searched_paths():
                click.echo(f"    - {path}")
            raise click.Abort()

        # Parse the profile
        try:
            profile_config = parser.parse_profile(target=dbt_target)
        except Exception as e:
            _error(f"Could not parse profile: {e}")
            if verbose:
                traceback.print_exc()
            raise click.Abort()

        # Display configuration
        _rule()
        click.echo(click.style("  Profile Configuration", bold=True))
        _rule()

        _row("Profile", profile_config.profile_name)
        _row("Target", profile_config.target_name)

        _rule()

        _row("Account", profile_config.account)
        _row("User", profile_config.user)
        _row("Role", profile_config.role or click.style("(not set)", dim=True))
        _row("Warehouse", profile_config.warehouse or click.style("(not set)", dim=True))
        _row("Database", profile_config.database or click.style("(not set)", dim=True))
        _row("Schema", profile_config.schema or click.style("(not set)", dim=True))
        _row("Auth Method", profile_config.get_auth_method())

        _rule()

        _row("profiles.yml", str(profiles_path))
        _row("dbt_project", str(project_file))

        _rule()
        click.echo()

        # Validate configuration
        warnings = []
        if not profile_config.role:
            warnings.append("No role specified - will use user's default role")
        if not profile_config.warehouse:
            warnings.append("No warehouse specified - queries may fail")
        if not profile_config.database:
            warnings.append("No database specified - must use --database flag")
        if not profile_config.schema:
            warnings.append("No schema specified - must use --schema flag")

        if warnings:
            click.echo(click.style("  ⚠ Warnings:", fg="yellow"))
            for warning in warnings:
                click.echo(f"    • {warning}")
            click.echo()

        # Test connection if requested
        if test_connection:
            click.echo("  Testing Snowflake connection...")

            try:
                from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig

                # Create config (use INFORMATION_SCHEMA to avoid permission issues)
                config = SnowflakeConfig.from_dbt_profile(
                    target=dbt_target,
                    database_override="INFORMATION_SCHEMA",
                    schema_override="TABLES",
                )

                from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeClient

                client = SnowflakeClient(config)

                # Test query
                result = client.execute_query("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")

                if not result.empty:
                    current_user = result.iloc[0, 0]
                    current_role = result.iloc[0, 1]
                    current_wh = result.iloc[0, 2]

                    click.echo()
                    click.echo(click.style("  ✓ Connection successful!", fg="green", bold=True))
                    click.echo(f"    Connected as: {current_user}")
                    click.echo(f"    Current role: {current_role}")
                    click.echo(f"    Warehouse: {current_wh or '(none)'}")
                else:
                    click.echo(click.style("  ✓ Connection successful!", fg="green", bold=True))

            except Exception as e:
                click.echo()
                _error(f"Connection failed: {e}")
                if verbose:
                    traceback.print_exc()
                raise click.ClickException("Could not connect to Snowflake")

        else:
            click.echo(click.style("  ✓ Configuration valid", fg="green", bold=True))
            click.echo(click.style("    Use --test-connection to verify Snowflake connectivity", dim=True))

        click.echo()

    except click.Abort:
        raise
    except click.ClickException:
        raise
    except Exception as e:
        _error(f"Unexpected error: {e}")
        if verbose:
            traceback.print_exc()
        raise click.ClickException(str(e))


def _rule(width: int = 50):
    """Display a horizontal rule."""
    click.echo("  " + "─" * width)


def _row(label: str, value: str):
    """Display a formatted row with label and value."""
    label_styled = click.style(f"{label}:", fg="cyan")
    click.echo(f"  {label_styled:<24} {value}")


def _error(message: str):
    """Display an error message."""
    click.echo(click.style(f"  ✗ {message}", fg="red"))
