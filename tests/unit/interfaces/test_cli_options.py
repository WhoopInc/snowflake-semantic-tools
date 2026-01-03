"""
Tests for CLI options module.
"""

import click
import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.interfaces.cli.options import (
    database_schema_options,
    defer_options,
    target_option,
    verbose_quiet_options,
)


class TestTargetOption:
    """Tests for target_option decorator."""

    def test_target_option_adds_flag(self):
        """Test that target_option adds --target and -t flags."""

        @click.command()
        @target_option
        def test_cmd(dbt_target):
            click.echo(f"target={dbt_target}")

        runner = CliRunner()

        # Test with --target
        result = runner.invoke(test_cmd, ["--target", "prod"])
        assert result.exit_code == 0
        assert "target=prod" in result.output

        # Test with -t shorthand
        result = runner.invoke(test_cmd, ["-t", "dev"])
        assert result.exit_code == 0
        assert "target=dev" in result.output

        # Test without flag (should be None)
        result = runner.invoke(test_cmd, [])
        assert result.exit_code == 0
        assert "target=None" in result.output


class TestDatabaseSchemaOptions:
    """Tests for database_schema_options decorator."""

    def test_database_schema_options_adds_flags(self):
        """Test that database_schema_options adds --db and --schema flags."""

        @click.command()
        @database_schema_options
        def test_cmd(db, schema):
            click.echo(f"db={db}, schema={schema}")

        runner = CliRunner()

        # Test with both options
        result = runner.invoke(test_cmd, ["--db", "ANALYTICS", "--schema", "PROD"])
        assert result.exit_code == 0
        assert "db=ANALYTICS" in result.output
        assert "schema=PROD" in result.output

        # Test with -s shorthand
        result = runner.invoke(test_cmd, ["--db", "DB", "-s", "SCHEMA"])
        assert result.exit_code == 0
        assert "schema=SCHEMA" in result.output

        # Test without flags (should be None)
        result = runner.invoke(test_cmd, [])
        assert result.exit_code == 0
        assert "db=None" in result.output
        assert "schema=None" in result.output


class TestDeferOptions:
    """Tests for defer_options decorator."""

    def test_defer_options_adds_all_flags(self):
        """Test that defer_options adds all defer-related flags."""

        @click.command()
        @defer_options
        def test_cmd(defer_target, state, only_modified, no_defer):
            click.echo(f"defer_target={defer_target}")
            click.echo(f"state={state}")
            click.echo(f"only_modified={only_modified}")
            click.echo(f"no_defer={no_defer}")

        runner = CliRunner()

        # Test with defer_target
        result = runner.invoke(test_cmd, ["--defer-target", "prod"])
        assert result.exit_code == 0
        assert "defer_target=prod" in result.output

        # Test with only_modified flag
        result = runner.invoke(test_cmd, ["--only-modified"])
        assert result.exit_code == 0
        assert "only_modified=True" in result.output

        # Test with no_defer flag
        result = runner.invoke(test_cmd, ["--no-defer"])
        assert result.exit_code == 0
        assert "no_defer=True" in result.output

        # Test defaults
        result = runner.invoke(test_cmd, [])
        assert result.exit_code == 0
        assert "defer_target=None" in result.output
        assert "only_modified=False" in result.output
        assert "no_defer=False" in result.output


class TestVerboseQuietOptions:
    """Tests for verbose_quiet_options decorator."""

    def test_verbose_quiet_options_adds_flags(self):
        """Test that verbose_quiet_options adds --verbose and --quiet flags."""

        @click.command()
        @verbose_quiet_options
        def test_cmd(verbose, quiet):
            click.echo(f"verbose={verbose}, quiet={quiet}")

        runner = CliRunner()

        # Test with --verbose
        result = runner.invoke(test_cmd, ["--verbose"])
        assert result.exit_code == 0
        assert "verbose=True" in result.output

        # Test with -v shorthand
        result = runner.invoke(test_cmd, ["-v"])
        assert result.exit_code == 0
        assert "verbose=True" in result.output

        # Test with --quiet
        result = runner.invoke(test_cmd, ["--quiet"])
        assert result.exit_code == 0
        assert "quiet=True" in result.output

        # Test with -q shorthand
        result = runner.invoke(test_cmd, ["-q"])
        assert result.exit_code == 0
        assert "quiet=True" in result.output

        # Test defaults
        result = runner.invoke(test_cmd, [])
        assert result.exit_code == 0
        assert "verbose=False" in result.output
        assert "quiet=False" in result.output


class TestCombinedOptions:
    """Tests for combining multiple option decorators."""

    def test_combined_decorators(self):
        """Test that multiple decorators can be combined."""

        @click.command()
        @target_option
        @database_schema_options
        @defer_options
        @verbose_quiet_options
        def test_cmd(dbt_target, db, schema, defer_target, state, only_modified, no_defer, verbose, quiet):
            click.echo(f"dbt_target={dbt_target}")
            click.echo(f"db={db}")
            click.echo(f"defer_target={defer_target}")
            click.echo(f"verbose={verbose}")

        runner = CliRunner()

        result = runner.invoke(
            test_cmd,
            ["--target", "prod", "--db", "ANALYTICS", "--defer-target", "prod", "--verbose"],
        )
        assert result.exit_code == 0
        assert "dbt_target=prod" in result.output
        assert "db=ANALYTICS" in result.output
        assert "defer_target=prod" in result.output
        assert "verbose=True" in result.output
