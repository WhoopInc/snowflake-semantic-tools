"""The shared click options: their names, types, and defaults, and the order they are applied in."""

from __future__ import annotations

from pathlib import Path

import click

from snowflake_semantic_tools.cli import options


def params(*decorators: options.Decorator) -> list[click.Parameter]:
    """Return the parameters of a command declared with `decorators`, written top to bottom."""

    @options.stacked(*decorators)
    def command() -> None:
        """Demonstrate the options."""

    return click.command()(command).params


def test_stacked_options_keep_the_order_they_are_written_in() -> None:
    declared = params(click.option("--a"), click.option("--b"), click.option("--c"))
    assert [param.name for param in declared] == ["a", "b", "c"]


def test_the_option_groups_expand_in_the_order_the_commands_list_them() -> None:
    declared = params(
        options.project_options(),
        options.selection_options(),
        options.validation_options(),
        options.output_option(),
    )
    assert [param.opts[0] for param in declared] == [
        "--project-dir",
        "--target",
        "--manifest",
        "--select",
        "--exclude",
        "--strict",
        "--snowflake-syntax-check",
        "--output",
    ]
    assert [param.name for param in declared][:5] == [
        "project_dir",
        "target_name",
        "manifest_path",
        "selected",
        "excluded",
    ]


def test_the_project_directory_must_exist_unless_the_command_creates_it() -> None:
    [checked] = params(options.project_dir_option())
    [created] = params(options.project_dir_option(exists=False, show_default=True))
    assert isinstance(checked, click.Option) and isinstance(created, click.Option)
    assert isinstance(checked.type, click.Path) and isinstance(created.type, click.Path)
    assert (checked.type.exists, checked.type.file_okay, checked.default, checked.show_default) == (
        True,
        False,
        Path("."),
        None,
    )
    assert (created.type.exists, created.type.file_okay, created.show_default) == (False, False, True)


def test_the_manifest_must_be_an_existing_file() -> None:
    [manifest] = params(options.manifest_option())
    assert isinstance(manifest.type, click.Path)
    assert (manifest.name, manifest.type.exists, manifest.type.dir_okay, manifest.multiple) == (
        "manifest_path",
        True,
        False,
        False,
    )


def test_select_repeats_except_where_a_command_takes_one_selector() -> None:
    [many] = params(options.select_option())
    [one] = params(options.select_option(multiple=False))
    assert (many.name, many.multiple, one.name, one.multiple) == ("selected", True, "selected", False)


def test_the_validation_switches_default_to_unset_and_the_flags_to_off() -> None:
    strict, syntax = params(options.validation_options())
    assert isinstance(strict, click.Option) and isinstance(syntax, click.Option)
    assert (strict.secondary_opts, strict.default) == (["--no-strict"], None)
    assert (syntax.secondary_opts, syntax.default) == (["--no-snowflake-syntax-check"], None)
    flags = params(options.prune_option(), options.partial_option(), options.fail_fast_option())
    assert [(param.name, isinstance(param, click.Option) and param.is_flag) for param in flags] == [
        ("prune", True),
        ("partial", True),
        ("fail_fast", True),
    ]


def test_sql_out_is_a_directory_and_output_defaults_to_human() -> None:
    sql_out, output = params(options.sql_out_option(), options.output_option())
    assert isinstance(sql_out.type, click.Path) and isinstance(output.type, click.Choice)
    assert (sql_out.name, sql_out.type.file_okay, sql_out.type.exists) == ("sql_out", False, False)
    assert (output.default, list(output.type.choices)) == ("human", ["human", "json"])
