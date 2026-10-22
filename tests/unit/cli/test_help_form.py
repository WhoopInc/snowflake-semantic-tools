"""`--help` is a one-screen summary; `--help --full` is the whole reference, as it was before."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.help_form import first_paragraph, first_sentence, fit, short_help_record
from snowflake_semantic_tools.cli.main import cli

REFERENCE = Path(__file__).resolve().parents[3] / "docs" / "reference" / "cli.md"
# The most lines any summary may take, laid out at 120 columns.
MOST_LINES = 40


def _commands(group: click.Group, path: tuple[str, ...] = ()) -> Iterator[tuple[tuple[str, ...], click.Command]]:
    for name, command in group.commands.items():
        if command.hidden:
            continue
        yield (*path, name), command
        if isinstance(command, click.Group):
            yield from _commands(command, (*path, name))


EVERY_PATH = [(), *(path for path, _ in _commands(cli))]


def _help(*args: str) -> click.testing.Result:
    # CliRunner forces 80 columns unless the context is given its own width.
    return CliRunner().invoke(cli, list(args), prog_name="sst", terminal_width=120)


@pytest.mark.parametrize(
    ("text", "sentence"),
    [
        ("One. Two.", "One."),
        ("No period at all", "No period at all"),
        ("Ends at the end.", "Ends at the end."),
        ("Reads `sst_config.yml` first. Then more.", "Reads `sst_config.yml` first."),
        ("dbt 1.10 to 1.13 work. Others do not.", "dbt 1.10 to 1.13 work."),
        ("Names a type, e.g. `view`, or i.e. one. Rest.", "Names a type, e.g. `view`, or i.e. one."),
        ("Quoted `a. b` stays. Rest.", "Quoted `a. b` stays."),
        ("Wrapped over\n  two lines. Rest.", "Wrapped over two lines."),
        ("Some extras, etc. and more. Rest.", "Some extras, etc. and more."),
        ("", ""),
    ],
)
def test_the_first_sentence_ends_at_a_real_full_stop(text: str, sentence: str) -> None:
    assert first_sentence(text) == sentence


def test_the_first_paragraph_stops_at_the_first_blank_line() -> None:
    assert first_paragraph("Summary line.\n\n    More prose.\n\n    Diagnostics:\n") == "Summary line."
    assert first_paragraph("  Two lines\n  of summary.\n  \n  Rest.") == "Two lines\nof summary."
    assert first_paragraph("Visible.\fHidden.") == "Visible."
    assert first_paragraph(None) == ""


def test_a_row_keeps_clicks_suffix_and_fits_its_width() -> None:
    assert short_help_record(("--n", "Sessions. More.  [default: 4]")) == ("--n", "Sessions.  [default: 4]")
    assert short_help_record(("--r", "  [required]")) == ("--r", "[required]")
    names, text = short_help_record(("--t", "word " * 30 + "end.  [1<=x<=16]"), 40)
    assert len(text) <= 40 and text.endswith("...  [1<=x<=16]")
    assert fit("short", 10) == "short" and fit("one two three, four", 14) == "one two..."
    assert fit("anything", None) == "anything"


@pytest.mark.parametrize("path", EVERY_PATH, ids=lambda path: " ".join(("sst", *path)))
def test_every_summary_fits_a_screen_and_points_at_the_full_reference(path: tuple[str, ...]) -> None:
    shown = _help(*path, "--help")
    assert shown.exit_code == 0, shown.output
    lines = shown.output.splitlines()
    assert len(lines) <= MOST_LINES, shown.output
    assert "Diagnostics:" not in shown.output
    anchor = "-".join(("sst", *path))
    pointer = " ".join(("sst", *path, "--help --full"))
    assert f"Full reference: {pointer}" in shown.output
    if path:
        assert f"docs/reference/cli.md#{anchor}" in shown.output
        assert f"\n## {' '.join(('sst', *path))}\n" in REFERENCE.read_text(encoding="utf-8")


@pytest.mark.parametrize("path", EVERY_PATH, ids=lambda path: " ".join(("sst", *path)))
def test_the_full_form_is_the_whole_reference_in_either_order(path: tuple[str, ...]) -> None:
    full = _help(*path, "--help", "--full")
    assert full.exit_code == 0, full.output
    assert _help(*path, "--full", "--help").output == full.output
    command: click.Command = cli
    for name in path:
        assert isinstance(command, click.Group)
        command = command.commands[name]
    # click wraps at spaces and after hyphens, so the text is compared without its whitespace.
    flat = "".join(full.output.split())
    assert "".join((command.help or "").split()).replace("\b", "") in flat
    for param in command.params:
        if isinstance(param, click.Option) and not param.hidden and param.help:
            assert "".join(param.help.split()) in flat, (path, param.opts)


def test_the_root_summary_drops_the_prose_the_full_form_keeps() -> None:
    short, full = _help("--help").output, _help("--help", "--full").output
    assert "A global option goes before the command" not in short
    assert "A global option goes before the command" in full
    assert "Commands:" in short and "  apply " in short
    assert "Global options:\n  --output, --verbose, --quiet, --log-level and" in short


def test_full_without_help_is_a_usage_error() -> None:
    for args in (("apply", "--full"), ("--full", "validate"), ("state", "--full")):
        refused = _help(*args)
        assert refused.exit_code == 3, refused.output
        assert "SST-PRT100" in refused.output and "--full goes with --help" in refused.output


def test_plan_keeps_its_own_full_flag() -> None:
    plan = cli.commands["plan"]
    with click.Context(plan) as ctx:
        fulls = [param for param in plan.get_params(ctx) if "--full" in param.opts]
    assert [param.name for param in fulls] == ["full"]
    assert "Exit 0 with nothing to change" in _help("plan", "--help", "--full").output
    assert "Exit 0 with nothing to change" not in _help("plan", "--help").output


def test_the_full_form_keeps_the_diagnostics_list() -> None:
    full, short = _help("apply", "--help", "--full").output, _help("apply", "--help").output
    assert "  Diagnostics:\n      SST-PRT109:" in full and "SST-PRT109" not in short
