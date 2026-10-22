"""The two forms of `--help`: a one-screen summary by default, and the whole reference with `--full`.

The summary is cut from the same text as the reference, so the two cannot drift: the command's
first paragraph, each option's first sentence with click's `[default: ...]` and `[required]`
kept, the global options folded to one line, and a pointer to the full form. `--full` prints
what `--help` printed before the summary existed; it is accepted only with `--help`, before or
after it. The reference pages `sst docs` writes read the help text itself, never this form.
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Sequence
from typing import Any

import click

FULL_FLAG = "--full"
# ctx.meta keys the dry parse sets: whether the command line asks for help, and for the full form.
_HELP_ASKED = "sst.help_asked"
_FULL_ASKED = "sst.full_help"
# Words a period ends without ending the sentence, compared case-insensitively without the period.
_ABBREVIATIONS = frozenset(("e.g", "i.e", "etc", "vs", "cf", "approx", "no"))
# click's suffixes on an option's help: `[default: 4]`, `[required]`, `[env var: X]`, and the like.
_EXTRAS = re.compile(r"(\s+\[[^\[\]]*\])+\s*$")
# How many global options the folded line names before it counts the rest.
_NAMED_GLOBALS = 4
_ELLIPSIS = "..."
# The widest the summary is laid out, however wide the terminal; the full form keeps click's 80.
SUMMARY_WIDTH = 120
# The narrowest help a one-line row is cut to, however long its suffix.
_MIN_ROOM = 20
# click's definition-list layout: the widest first column, and the gaps around the second.
_COL_MAX = 30
_COL_GAPS = 4


def first_paragraph(text: str | None) -> str:
    """Return `text` up to its first blank line, as `inspect.cleandoc` dedents it."""
    if not text:
        return ""
    cleaned = inspect.cleandoc(text).split("\f", 1)[0]
    return re.split(r"\n\s*\n", cleaned, maxsplit=1)[0].strip()


def first_sentence(text: str) -> str:
    """Return the first sentence of `text`, joined onto one line.

    A sentence ends at a period followed by whitespace or the end of the text, unless the period
    is inside backticks or ends an abbreviation such as `e.g.`; a period inside a word, as in
    `1.10` or `sst_config.yml`, never ends one.
    """
    flat = " ".join(text.split())
    quoted = False
    for index, char in enumerate(flat):
        if char == "`":
            quoted = not quoted
        elif char == "." and not quoted and (index + 1 == len(flat) or flat[index + 1] == " "):
            word = flat[:index].rsplit(" ", 1)[-1].lstrip("(`").casefold()
            if word not in _ABBREVIATIONS:
                return flat[: index + 1]
    return flat


def short_help_record(record: tuple[str, str], width: int | None = None) -> tuple[str, str]:
    """Cut an option's help row to its first sentence, keeping click's trailing `[...]` suffixes.

    With `width`, the sentence is cut further, at a word, so the row's help fits on one line.
    """
    names, text = record
    extras = _EXTRAS.search(text)
    body, suffix = (text[: extras.start()], extras.group(0).strip()) if extras else (text, "")
    room = None if width is None else max(width - len(suffix) - 2 * bool(suffix), _MIN_ROOM)
    short = fit(first_sentence(body), room)
    return names, f"{short}  {suffix}" if short and suffix else short or suffix


def fit(text: str, width: int | None) -> str:
    """Return `text`, or as many of its words as fit in `width` with `...` after them."""
    if width is None or len(text) <= width:
        return text
    cut = text[: width - len(_ELLIPSIS) + 1].rsplit(" ", 1)[0].rstrip(",;:")
    return f"{cut}{_ELLIPSIS}"


def global_line(names: Sequence[str]) -> str:
    """Fold the global options to one sentence: the first few by name, then how many more."""
    named = ", ".join(names[:_NAMED_GLOBALS])
    rest = len(names) - _NAMED_GLOBALS
    more = f" and {rest} more" if rest > 0 else ""
    return f"{named}{more}; see --help --full."


def reference_pointer(ctx: click.Context) -> str:
    """Say where the full reference is: the `--full` form, and the command's reference page section."""
    names: list[str] = []
    current: click.Context | None = ctx
    while current is not None and current.parent is not None:
        names.insert(0, current.info_name or "")
        current = current.parent
    # The page heads each command `sst ...`, however the program was started.
    page = f"docs/reference/cli.md#{'-'.join(('sst', *names))}" if names else "docs/reference/cli.md"
    return f"Full reference: {ctx.command_path} --help --full, or {page}"


def _asked(command: click.Command, ctx: click.Context, args: Sequence[str]) -> tuple[bool, bool]:
    """Parse `args` without effect, and say whether they ask for `--help` and for `--full`.

    A command line that does not parse asks for neither: the real parse reports it.
    """
    parser = command.make_parser(ctx)
    try:
        opts, _, _ = parser.parse_args(args=list(args))
    except click.UsageError:
        return False, False
    return bool(opts.get("help")), bool(opts.get(_full_name(command)))


def _full_name(command: click.Command) -> str:
    """Name the parameter `--full` sets: the command's own, as `sst plan --full`, else the hidden one."""
    own = (param for param in command.params if FULL_FLAG in param.opts and param.name)
    return next((str(param.name) for param in own), "full_help")


class ConciseHelp(click.Command):
    """A command whose `--help` is a summary, and whose `--help --full` is the whole reference.

    Used as a base before `click.Command` or `click.Group`; `is_global` says which options fold
    into the one `Global options` line.

    Diagnostics:
        SST-PRT100: --full without --help.
    """

    def is_global(self, param: click.Parameter) -> bool:
        """Whether `param` is a global option, folded into one line in the summary."""
        return False

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        """Note whether the command line asks for help, and its full form, before click acts on it."""
        ctx.meta[_HELP_ASKED], ctx.meta[_FULL_ASKED] = _asked(self, ctx, args)
        return super().parse_args(ctx, args)

    def get_params(self, ctx: click.Context) -> list[click.Parameter]:
        """Add the hidden `--full`, which only the help option reads, unless the command has its own."""
        params = super().get_params(ctx)
        if _full_name(self) != "full_help":
            return params
        return [*params, self._full_option()]

    def _full_option(self) -> click.Option:
        return click.Option(
            [FULL_FLAG, "full_help"],
            is_flag=True,
            hidden=True,
            expose_value=False,
            is_eager=True,
            help="With --help, print the whole reference.",
            callback=_refuse_full_alone,
        )

    def get_help(self, ctx: click.Context) -> str:
        """Render the help; the summary may use a terminal up to `SUMMARY_WIDTH` wide, the reference 80."""
        if ctx.meta.get(_FULL_ASKED):
            return super().get_help(ctx)
        formatter = click.HelpFormatter(width=ctx.terminal_width, max_width=SUMMARY_WIDTH)
        self.format_help(ctx, formatter)
        return formatter.getvalue().rstrip("\n")

    def format_help(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        """Write the summary, or the whole reference when the command line asks for `--full`."""
        if ctx.meta.get(_FULL_ASKED):
            super().format_help(ctx, formatter)
            return
        self.format_usage(ctx, formatter)
        description = first_paragraph(self.help)
        if description:
            formatter.write_paragraph()
            with formatter.indentation():
                formatter.write_text(description)
        own: list[tuple[str, str]] = []
        globals_: list[str] = []
        for param in self.get_params(ctx):
            record = param.get_help_record(ctx)
            if record is None:
                continue
            if self.is_global(param):
                globals_.append(_long_name(param))
            else:
                own.append(record)
        if own:
            first_col = min(max(len(names) for names, _ in own), _COL_MAX)
            # The help column is as wide as click makes it inside the section's indentation.
            room = formatter.width - formatter.current_indent - 2 - first_col - _COL_GAPS
            with formatter.section("Options"):
                formatter.write_dl([short_help_record(row, room) for row in own], col_max=_COL_MAX)
        if isinstance(self, click.Group):
            self.format_commands(ctx, formatter)
        if globals_:
            with formatter.section("Global options"):
                formatter.write_text(global_line(globals_))
        formatter.write_paragraph()
        formatter.write_text(reference_pointer(ctx))


def _long_name(param: click.Parameter) -> str:
    """Name an option by its first long spelling, as the folded global line lists it."""
    return next((opt for opt in param.opts if opt.startswith("--")), param.opts[0])


def _refuse_full_alone(ctx: click.Context, param: click.Parameter, value: Any) -> None:
    if value and not ctx.resilient_parsing and not ctx.meta.get(_HELP_ASKED):
        # Imported here: `cli.group` imports `cli.globals`, which imports this module.
        from snowflake_semantic_tools.cli.group import SstUsageError

        raise SstUsageError(f"{FULL_FLAG} goes with --help: write {ctx.command_path} --help --full", ctx)


class SstSubGroup(ConciseHelp, click.Group):
    """A group of `sst` subcommands, such as `sst state`, with the same two forms of help."""
