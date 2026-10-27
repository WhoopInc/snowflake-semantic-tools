"""The conversation's color themes: `sst-dark`, its default, and every theme Textual has.

`sst-dark` is black, with white text, a mint green for what is primary or accented and for
success, a sky blue for what is secondary, a red for errors and an amber for warnings.
Ctrl-P's "Change theme" lists it beside Textual's own for the session.
"""

from __future__ import annotations

from textual.theme import BUILTIN_THEMES, Theme

DEFAULT_THEME = "sst-dark"

SST_DARK = Theme(
    name=DEFAULT_THEME,
    primary="#00F19F",
    secondary="#29B5E8",
    accent="#00F19F",
    warning="#F5A623",
    error="#EF4444",
    success="#00F19F",
    foreground="#FFFFFF",
    background="#000000",
    surface="#0E0E0E",
    panel="#1F1F1F",
    dark=True,
    variables={
        "text-muted": "#CCCCCC",
        "text-disabled": "#6B7280",
        "border": "#1F1F1F",
        "border-blurred": "#1F1F1F",
        "footer-key-foreground": "#00F19F",
        "input-selection-background": "#00F19F 35%",
        "block-cursor-background": "#00F19F",
        "block-cursor-foreground": "#000000",
    },
)

THEMES: dict[str, Theme] = {DEFAULT_THEME: SST_DARK}


def theme_names() -> tuple[str, ...]:
    """Return every theme the conversation can take: `sst-dark` first, then Textual's, sorted."""
    return (*THEMES, *sorted(name for name in BUILTIN_THEMES if name not in THEMES))
