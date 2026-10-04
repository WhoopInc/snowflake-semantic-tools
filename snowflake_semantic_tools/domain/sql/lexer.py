"""A small Snowflake SQL lexer: enough to tell where strings, identifiers, and comments are.

It does not parse. It splits text into tokens so a guard can ask what lies outside every
string and quoted identifier: single-quoted strings (with `''` and backslash escapes),
double-quoted identifiers (with `""`), `$$` bodies, `--`, `//`, and `/* */` comments, words,
numbers, whitespace, and one-character punctuation.

A `--` or `//` comment ends at `\n` or `\r\n`. Readers disagree on whether any other line break
ends one, so a comment holding one does not lex: SST never guesses where Snowflake resumes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

# Every character some SQL or Unicode reader takes as a line break (those `str.splitlines`
# splits on). A line comment ends at the first one; it must be `\n` or begin `\r\n`.
_LINE_BREAK = re.compile("[\n\r\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029]")


class TokenKind(Enum):
    """What one token is."""

    WORD = "word"
    NUMBER = "number"
    STRING = "string"
    QUOTED_IDENTIFIER = "quoted_identifier"
    DOLLAR_STRING = "dollar_string"
    COMMENT = "comment"
    SPACE = "space"
    PUNCTUATION = "punctuation"


@dataclass(frozen=True, slots=True)
class Token:
    """One token, with the offset of its first character in the lexed text."""

    kind: TokenKind
    text: str
    offset: int


class LexError(ValueError):
    """Text that does not lex: an unterminated string, identifier, or comment, or a NUL.

    Attributes:
        offset: Where the problem starts, as an index into the text.
        reason: What is wrong, as a phrase.
    """

    def __init__(self, offset: int, reason: str) -> None:
        super().__init__(f"{reason} at offset {offset}")
        self.offset = offset
        self.reason = reason


def _word_character(character: str) -> bool:
    return character.isascii() and (character.isalnum() or character in "_$")


def _quoted_end(text: str, start: int) -> int:
    """The index just past the single-quoted string opening at `start`.

    A backslash escapes the character after it, and a doubled quote is one quote.

    Raises:
        LexError: the string is never closed.
    """
    index = start + 1
    while index < len(text):
        character = text[index]
        if character == "\\":
            index += 2
        elif character == "'":
            if text.startswith("'", index + 1):
                index += 2
            else:
                return index + 1
        else:
            index += 1
    raise LexError(start, "unterminated string literal")


def _identifier_end(text: str, start: int) -> int:
    index = start + 1
    while True:
        close = text.find('"', index)
        if close < 0:
            raise LexError(start, "unterminated quoted identifier")
        if not text.startswith('"', close + 1):
            return close + 1
        index = close + 2


def _delimited_end(text: str, start: int, opener: str, closer: str, what: str) -> int:
    close = text.find(closer, start + len(opener))
    if close < 0:
        raise LexError(start, f"unterminated {what}")
    return close + len(closer)


def _line_comment_end(text: str, start: int) -> int:
    """The index of the line break ending the `--` or `//` comment at `start`, or the text's end.

    Raises:
        LexError: the comment is ended by a line break other than `\n` or `\r\n`.
    """
    found = _LINE_BREAK.search(text, start)
    if found is None:
        return len(text)
    end = found.start()
    if text[end] != "\n" and not text.startswith("\r\n", end):
        raise LexError(end, f"line break {text[end]!r} in a comment")
    return end


def _token_end(text: str, index: int) -> tuple[TokenKind, int]:
    """Classify the token starting at `index`, and find where it ends.

    Raises:
        LexError: the token is a string, identifier, or comment that never closes, a line
            comment ended by an ambiguous line break, or a NUL.
    """
    character = text[index]
    two = text[index : index + 2]
    if character == "\x00":
        raise LexError(index, "NUL character")
    if character == "'":
        return TokenKind.STRING, _quoted_end(text, index)
    if character == '"':
        return TokenKind.QUOTED_IDENTIFIER, _identifier_end(text, index)
    if two == "$$":
        return TokenKind.DOLLAR_STRING, _delimited_end(text, index, "$$", "$$", "dollar-quoted string")
    if two in ("--", "//"):
        return TokenKind.COMMENT, _line_comment_end(text, index)
    if two == "/*":
        return TokenKind.COMMENT, _delimited_end(text, index, "/*", "*/", "block comment")
    end = index + 1
    if character.isspace():
        while end < len(text) and text[end].isspace():
            end += 1
        return TokenKind.SPACE, end
    if character.isascii() and (character.isalpha() or character == "_"):
        kind = TokenKind.WORD
    elif character.isascii() and character.isdigit():
        kind = TokenKind.NUMBER
    else:
        return TokenKind.PUNCTUATION, end
    # A word stops before `$$`, which Snowflake may read as opening a dollar-quoted string.
    while end < len(text) and _word_character(text[end]) and not text.startswith("$$", end):
        end += 1
    return kind, end


def tokenize(text: str) -> tuple[Token, ...]:
    """Split text into tokens, in order; concatenating their text gives the text back.

    Raises:
        LexError: a string, quoted identifier, `$$` body, or block comment is never closed, a
            line comment is ended by a line break other than `\n` or `\r\n`, or the text holds
            a NUL.
    """
    tokens: list[Token] = []
    index = 0
    while index < len(text):
        kind, end = _token_end(text, index)
        tokens.append(Token(kind, text[index:end], index))
        index = end
    return tuple(tokens)
