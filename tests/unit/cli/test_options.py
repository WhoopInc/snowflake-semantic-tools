"""The shared click options: their names, types, and defaults, and the order they are applied in."""

from __future__ import annotations

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
    declared = params(options.target_option(), options.selection_options(), options.validation_options())
    assert [param.opts[0] for param in declared] == [
        "--target",
        "--select",
        "--exclude",
        "--strict",
        "--snowflake-syntax-check",
    ]
    assert [param.name for param in declared][:3] == ["target_name", "selected", "excluded"]
    target, _, _, strict, _ = declared
    assert isinstance(target, click.Option) and target.opts == ["--target", "-t"] and target.envvar == "SST_TARGET"
    assert isinstance(strict, click.Option) and strict.envvar == "SST_STRICT"


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
    [pair] = params(options.fail_fast_pair())
    assert isinstance(pair, click.Option) and (pair.secondary_opts, pair.default) == (["--no-fail-fast"], None)


def test_sql_out_and_state_are_directories_and_threads_is_bounded() -> None:
    sql_out, state, threads = params(options.sql_out_option(), options.state_option(), options.threads_option())
    assert isinstance(sql_out.type, click.Path) and isinstance(state.type, click.Path)
    assert (sql_out.name, sql_out.type.file_okay, sql_out.type.exists) == ("sql_out", False, False)
    assert (state.name, state.envvar) == ("state_dir", "SST_STATE_DIR")
    assert isinstance(threads.type, click.IntRange) and (threads.type.min, threads.type.max) == (1, 16)
