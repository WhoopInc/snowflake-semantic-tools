"""What the terminal can show, decided once per run from the environment and the two streams."""

from __future__ import annotations

import io

import pytest

from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.render.terminal import (
    MIN_WIDTH,
    PLAIN,
    Terminal,
    agent_marker,
    detect_terminal,
    system_terminal,
)


class _Stream(io.StringIO):
    def __init__(self, tty: bool, encoding: str = "utf-8") -> None:
        super().__init__()
        self._tty = tty
        self._encoding = encoding

    def isatty(self) -> bool:
        return self._tty

    @property
    def encoding(self) -> str:  # type: ignore[override]
        return self._encoding


def _detect(
    environ: dict[str, str], *, out: bool = True, err: bool = True, choice: str = "auto", colorable: bool = True
) -> Terminal:
    return detect_terminal(environ, _Stream(out), _Stream(err), choice=choice, colorable=colorable)


def test_a_terminal_gets_colour_and_a_live_line() -> None:
    terminal = _detect({"COLUMNS": "120"})
    assert terminal.color and terminal.color_out and terminal.live and terminal.unicode
    assert terminal.width == 120


def test_a_pipe_gets_neither_colour_nor_a_live_line() -> None:
    terminal = _detect({}, out=False, err=False)
    assert not terminal.color and not terminal.color_out and not terminal.live


def test_stdout_and_stderr_are_decided_apart() -> None:
    terminal = _detect({}, out=False, err=True)
    assert terminal.color and not terminal.color_out


@pytest.mark.parametrize(
    ("environ", "choice", "expected"),
    [
        ({"NO_COLOR": "1"}, "auto", False),
        ({"NO_COLOR": ""}, "auto", True),
        ({"TERM": "dumb"}, "auto", False),
        ({"NO_COLOR": "1"}, "always", True),
        ({}, "never", False),
    ],
)
def test_colour_follows_the_choice_then_the_environment(environ: dict[str, str], choice: str, expected: bool) -> None:
    assert _detect(environ, choice=choice).color is expected


@pytest.mark.parametrize("variable", ["FORCE_COLOR", "CLICOLOR_FORCE"])
def test_a_forcing_variable_colours_a_pipe(variable: str) -> None:
    assert _detect({variable: "1"}, err=False).color
    assert not _detect({variable: "0"}, err=False).color


def test_an_uncolourable_format_is_never_coloured() -> None:
    assert not _detect({}, choice="always", colorable=False).color


def test_ascii_stands_in_where_the_stream_cannot_encode_unicode() -> None:
    terminal = detect_terminal({}, _Stream(True), _Stream(True, encoding="ascii"))
    style = Style(terminal)
    assert not terminal.unicode
    assert style.symbol("ok") == "ok" and style.sep == " - " and style.spinner == "|/-\\"


def test_a_narrow_width_is_raised_to_the_minimum() -> None:
    assert _detect({"COLUMNS": "20"}).width == MIN_WIDTH


@pytest.mark.parametrize(
    ("environ", "expected"),
    [
        ({"TERM_PROGRAM": "iTerm.app"}, True),
        ({"TERM": "xterm-kitty"}, True),
        ({"TERM_PROGRAM": "iTerm.app", "TMUX": "/tmp/x"}, False),
        ({"TERM_PROGRAM": "Apple_Terminal"}, False),
    ],
)
def test_hyperlinks_only_where_the_terminal_renders_them(environ: dict[str, str], expected: bool) -> None:
    assert _detect(environ).hyperlinks is expected


def test_a_link_is_plain_text_without_hyperlinks() -> None:
    assert Style(PLAIN).link("SST-REF002", "https://x") == "SST-REF002"
    linked = Style(Terminal(color=True, hyperlinks=True)).link("SST-REF002", "https://x")
    assert linked == "\x1b]8;;https://x\x1b\\SST-REF002\x1b]8;;\x1b\\"


def test_style_paints_only_when_colour_is_on() -> None:
    assert Style(PLAIN)("text", "error") == "text"
    assert Style(Terminal(color=True))("text", "error") != "text"
    assert Style(Terminal(color=True))("", "error") == ""
    assert Style(Terminal(color_out=True), stdout=True)("text", "dim") != "text"


@pytest.mark.parametrize(
    ("environ", "tty", "expected"),
    [
        ({"COCO_AGENT": "1"}, False, "COCO_AGENT"),
        ({"CLAUDECODE": "1"}, False, "CLAUDECODE"),
        ({"COCO_AGENT": "1"}, True, None),
        ({"COCO_AGENT": "0"}, False, None),
        ({"SST_AGENT": ""}, False, None),
        ({}, False, None),
    ],
)
def test_an_agent_marker_counts_only_with_stdout_off_a_terminal(
    environ: dict[str, str], tty: bool, expected: str | None
) -> None:
    assert agent_marker(environ, _Stream(tty)) == expected


def test_a_stream_whose_isatty_fails_is_not_a_terminal() -> None:
    class Broken(io.StringIO):
        def isatty(self) -> bool:
            raise ValueError("closed")

    assert not detect_terminal({}, Broken(), Broken()).live


def test_the_process_terminal_is_detected_from_its_own_streams() -> None:
    assert isinstance(system_terminal(choice="never"), Terminal)
    assert not system_terminal(choice="never").color
