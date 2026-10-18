"""The look of `sst`'s terminal output: one palette and one set of symbols, each with an ASCII form.

Every renderer styles text through `Style`, so the whole look changes here. The palette uses
the 16 basic ANSI colours only, which every terminal theme maps to something readable on its
own background, light or dark.
"""

from __future__ import annotations

import click

from snowflake_semantic_tools.cli.render.terminal import Terminal

# Each role's foreground colour, bold, and dim: what it marks, never decoration.
_ROLES: dict[str, tuple[str | None, bool, bool]] = {
    "error": ("red", True, False),
    "warning": ("yellow", True, False),
    "info": ("cyan", True, False),
    "help": ("cyan", False, False),
    "success": ("green", True, False),
    "changes": ("yellow", True, False),
    "path": (None, True, False),
    "strong": (None, True, False),
    "dim": (None, False, True),
    "gutter": ("blue", True, False),
    "create": ("green", False, False),
    "update": ("yellow", False, False),
    "prune": ("red", False, False),
    "blocked": ("red", True, False),
    "before": ("red", False, False),
    "after": ("green", False, False),
}
# Each symbol as Unicode, then as the ASCII a stream that cannot encode it gets.
_SYMBOLS: dict[str, tuple[str, str]] = {
    "ok": ("✓", "ok"),
    "fail": ("✗", "x"),
    "changes": ("~", "~"),
    "arrow": ("→", "->"),
    "sep": (" · ", " - "),
    "ellipsis": ("…", "..."),
    "rule": ("─", "-"),
}
SPINNER_UNICODE = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
SPINNER_ASCII = "|/-\\"


class Style:
    """Paint text by role and pick symbols, as one terminal can show them.

    Args:
        stdout: Style for stdout, whose colour can differ from stderr's when one is redirected.
    """

    def __init__(self, terminal: Terminal, *, stdout: bool = False) -> None:
        self.terminal = terminal
        self.enabled = terminal.color_out if stdout else terminal.color

    def __call__(self, text: str, role: str) -> str:
        """Return `text` in `role`'s style, or as it is when colour is off."""
        if not self.enabled or not text:
            return text
        fg, bold, dim = _ROLES[role]
        return click.style(text, fg=fg, bold=bold or None, dim=dim or None)

    def symbol(self, name: str) -> str:
        """Return the symbol `name`, in ASCII when the terminal cannot show Unicode."""
        unicode, ascii_form = _SYMBOLS[name]
        return unicode if self.terminal.unicode else ascii_form

    @property
    def sep(self) -> str:
        """Return the separator between the parts of one line: ` · `, or ` - ` in ASCII."""
        return self.symbol("sep")

    @property
    def spinner(self) -> str:
        """Return the frames a live progress line cycles through."""
        return SPINNER_UNICODE if self.terminal.unicode else SPINNER_ASCII

    def link(self, text: str, url: str) -> str:
        """Return `text` as an OSC 8 hyperlink to `url` where the terminal renders one, else `text`."""
        if not self.terminal.hyperlinks or not url:
            return text
        return f"\x1b]8;;{url}\x1b\\{text}\x1b]8;;\x1b\\"
