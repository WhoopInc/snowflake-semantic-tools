"""
Drop Command

CLI command for removing semantic views from Snowflake.
Supports dropping specific views or pruning orphaned views.
"""

import re

import click

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.interfaces.cli.options import database_schema_options, target_option
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.interfaces.cli.utils import (
    build_snowflake_config,
    get_target_database_schema,
    setup_command,
)
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("cli.drop")


@click.command(
    short_help="Remove semantic views from Snowflake",
)
@target_option
@database_schema_options
@click.argument("view_name", required=False)
@click.option("--prune", is_flag=True, help="Drop all orphaned views not tracked in SM_SEMANTIC_VIEWS")
@click.option("--dry-run", is_flag=True, help="Show what would be dropped without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt (for CI)")
@click.option("--verbose", "-V", is_flag=True, help="Show detailed output")
@click.pass_context
def drop(ctx, dbt_target, db, schema, view_name, prune, dry_run, yes, verbose):
    """Remove semantic views from Snowflake.

    \b
    Two modes:
      sst drop VIEW_NAME          Drop a specific semantic view
      sst drop --prune            Find and drop orphaned views

    \b
    Examples:
      sst drop OLD_VIEW_NAME --target prod
      sst drop --prune --dry-run --target prod
      sst drop --prune --yes --target prod       (CI mode, no prompt)

    \b
    Safety:
      --prune without --yes shows a confirmation prompt before dropping.
      --dry-run shows what would be dropped without executing.

    \b
    Next step: sst generate --all (to recreate views if needed)
    """
    setup_command(ctx)
    output = CLIOutput(verbose=verbose)
    output.header(f"sst drop (v{__version__})")

    if not view_name and not prune:
        raise click.UsageError("Specify a VIEW_NAME to drop, or use --prune to remove orphaned views.")

    if view_name and prune:
        raise click.UsageError("Cannot use both VIEW_NAME and --prune. Choose one mode.")

    config = build_snowflake_config(dbt_target)
    database, schema_name = get_target_database_schema(dbt_target, db, schema)

    from snowflake_semantic_tools.interfaces.cli.utils import snowflake_session

    try:
        with snowflake_session(config=config) as client:
            if view_name:
                success = _drop_specific_view(client, database, schema_name, view_name, dry_run, output)
            elif prune:
                success = _prune_orphaned_views(client, database, schema_name, dry_run, yes, verbose, output)
            else:
                success = True
    except Exception as e:
        output.error(str(e))
        raise click.Abort()

    if not success:
        raise SystemExit(1)


_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _drop_specific_view(client, database: str, schema: str, view_name: str, dry_run: bool, output: CLIOutput) -> bool:
    """Drop a single named semantic view."""
    if not _VALID_IDENTIFIER.match(view_name):
        output.error(f"Invalid view name: '{view_name}'. Must be a valid SQL identifier.")
        return False

    fqn = f"{database}.{schema}.{view_name.upper()}"

    if dry_run:
        output.info(f"Would drop: {fqn}")
        output.info("DRY RUN — no views were dropped.")
        return True

    sql = f"DROP SEMANTIC VIEW IF EXISTS {fqn}"
    try:
        client.execute_query(sql)
        output.success(f"Dropped: {fqn}")
        return True
    except Exception as e:
        output.error(f"Failed to drop {fqn}: {e}")
        return False


def _prune_orphaned_views(
    client, database: str, schema: str, dry_run: bool, yes: bool, verbose: bool, output: CLIOutput
) -> bool:
    """Find and drop orphaned semantic views (not tracked in SM_SEMANTIC_VIEWS)."""
    output.info(f"Scanning semantic views in: {database}.{schema}")

    df = client.execute_query(f"SHOW SEMANTIC VIEWS IN {database}.{schema}")
    actual_views = set(df["name"].str.upper().tolist()) if not df.empty else set()

    sm_table = f"{database}.{schema}.SM_SEMANTIC_VIEWS"
    try:
        tracked_df = client.execute_query(f"SELECT UPPER(NAME) AS NAME FROM {sm_table}")
        tracked_views = set(tracked_df["NAME"].tolist()) if not tracked_df.empty else set()
    except Exception:
        output.warning(f"Could not read {sm_table} — assuming no tracked views")
        tracked_views = set()

    orphaned = sorted(actual_views - tracked_views)

    output.info(f"Found {len(actual_views)} semantic views in schema")
    output.info(f"Found {len(tracked_views)} views in SM_SEMANTIC_VIEWS tracking table")

    if not orphaned:
        output.success("No orphaned semantic views found. Schema is clean.")
        return True

    output.info(f"\n{len(orphaned)} orphaned semantic view(s) detected:")
    for name in orphaned:
        output.info(f"  • {name}")

    if dry_run:
        output.info("\nDRY RUN — no views were dropped. Re-run without --dry-run to drop.")
        return True

    if not yes:
        click.confirm(f"\nDrop {len(orphaned)} orphaned view(s)?", abort=True)

    dropped = 0
    for name in orphaned:
        fqn = f"{database}.{schema}.{name}"
        try:
            client.execute_query(f"DROP SEMANTIC VIEW IF EXISTS {fqn}")
            output.success(f"Dropped: {fqn}")
            dropped += 1
        except Exception as e:
            output.error(f"Failed to drop {fqn}: {e}")

    output.success(f"\nDropped {dropped}/{len(orphaned)} orphaned view(s).")
    return dropped == len(orphaned)
