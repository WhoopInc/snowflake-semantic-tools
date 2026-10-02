"""Properties every rendered payload must have before SST publishes it, shared by the renderers.

Each is a pure predicate over text the renderer is about to emit, so the render phase can
report SST-RND002, SST-RND003 or SST-RND011 for the artifact instead of raising.
"""

from __future__ import annotations

# The statement size SST assumes Snowflake accepts. It is a guess, not a documented limit:
# Snowflake's own refusal of an oversized statement is what decides.
STATEMENT_SIZE_GUESS = 1_048_576


def unquotable(name: str) -> bool:
    """Report whether no quoting can carry `name`: it holds a NUL or a lone surrogate."""
    return any(character == "\x00" or 0xD800 <= ord(character) <= 0xDFFF for character in name)


def dollar_quote_offset(text: str) -> int | None:
    """Return the offset of the first `$$` in `text`, which would end a dollar-quoted body; None when absent."""
    offset = text.find("$$")
    return offset if offset >= 0 else None


def statement_size(text: str) -> int:
    """Return the size Snowflake receives a statement at: its UTF-8 bytes."""
    return len(text.encode("utf-8", errors="surrogatepass"))
