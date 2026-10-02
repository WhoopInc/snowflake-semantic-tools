"""Structured scanner for SST's ``{{ function('arg') }}`` reference dialect."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TemplateCall:
    """One parsed template call with its exact source span."""

    function: str
    args: tuple[str, ...]
    raw: str
    start: int
    end: int
    line: int
    col: int


class TemplateSyntaxError(ValueError):
    """A template span is unbalanced or does not match the call grammar."""

    def __init__(self, reason: str, *, line: int, col: int) -> None:
        super().__init__(f"{line}:{col}: {reason}")
        self.reason = reason
        self.line = line
        self.col = col


def _position(text: str, offset: int) -> tuple[int, int]:
    line = text.count("\n", 0, offset) + 1
    previous_newline = text.rfind("\n", 0, offset)
    return line, offset - previous_newline


def _syntax(text: str, offset: int, reason: str) -> TemplateSyntaxError:
    line, col = _position(text, offset)
    return TemplateSyntaxError(reason, line=line, col=col)


def _parse_body(body: str, *, text: str, body_offset: int) -> tuple[str, tuple[str, ...]]:
    """Parse what lies between one call's braces as `name('arg', ...)`; return the name and arguments.

    The name is a letter or underscore followed by letters, digits, or underscores. Each argument
    is a single- or double-quoted string in which a backslash escapes the next character, and it
    is returned unquoted and unescaped. Whitespace may surround any token, `name()` takes no
    arguments, and nothing may follow the closing parenthesis.

    Args:
        text: The whole scalar the call is in, so an error can give its line and column.
        body_offset: Where `body` starts in `text`.

    Raises:
        TemplateSyntaxError: the body breaks that grammar; the error points where it breaks.
    """
    cursor = 0
    length = len(body)

    def skip_space() -> None:
        nonlocal cursor
        while cursor < length and body[cursor].isspace():
            cursor += 1

    skip_space()
    name_start = cursor
    if cursor >= length or not (body[cursor].isalpha() or body[cursor] == "_"):
        raise _syntax(text, body_offset + cursor, "expected a template function name")
    cursor += 1
    while cursor < length and (body[cursor].isalnum() or body[cursor] == "_"):
        cursor += 1
    function = body[name_start:cursor]
    skip_space()
    if cursor >= length or body[cursor] != "(":
        raise _syntax(text, body_offset + cursor, f"expected '(' after {function}")
    cursor += 1
    skip_space()

    args: list[str] = []
    if cursor < length and body[cursor] == ")":
        cursor += 1
    else:
        while True:
            value, cursor = _quoted(body, cursor, text=text, body_offset=body_offset)
            args.append(value)
            skip_space()
            if cursor < length and body[cursor] == ",":
                cursor += 1
                skip_space()
                continue
            if cursor < length and body[cursor] == ")":
                cursor += 1
                break
            raise _syntax(text, body_offset + cursor, "expected ',' or ')' after template argument")

    skip_space()
    if cursor != length:
        raise _syntax(text, body_offset + cursor, "unexpected text after template call")
    return function, tuple(args)


def _quoted(body: str, cursor: int, *, text: str, body_offset: int) -> tuple[str, int]:
    """Read the quoted argument that opens at `cursor`; return it unescaped, and the offset just past it.

    Raises:
        TemplateSyntaxError: `cursor` is not at a quote, or the string never closes.
    """
    length = len(body)
    if cursor >= length or body[cursor] not in ("'", '"'):
        raise _syntax(text, body_offset + cursor, "template arguments must be quoted strings")
    quote = body[cursor]
    cursor += 1
    value: list[str] = []
    while cursor < length:
        char = body[cursor]
        if char == "\\" and cursor + 1 < length:
            value.append(body[cursor + 1])
            cursor += 2
            continue
        if char == quote:
            return "".join(value), cursor + 1
        value.append(char)
        cursor += 1
    raise _syntax(text, body_offset + cursor, "unterminated quoted template argument")


def scan_template_calls(text: str) -> tuple[TemplateCall, ...]:
    """Parse every template call in source order, retaining source positions."""
    calls: list[TemplateCall] = []
    cursor = 0
    while True:
        start = text.find("{{", cursor)
        if start < 0:
            return tuple(calls)
        end = text.find("}}", start + 2)
        if end < 0:
            raise _syntax(text, start, "unterminated template expression")
        nested = text.find("{{", start + 2, end)
        if nested >= 0:
            raise _syntax(text, nested, "nested template expression")
        raw = text[start : end + 2]
        function, args = _parse_body(raw[2:-2], text=text, body_offset=start + 2)
        line, col = _position(text, start)
        calls.append(
            TemplateCall(
                function=function,
                args=args,
                raw=raw,
                start=start,
                end=end + 2,
                line=line,
                col=col,
            )
        )
        cursor = end + 2


def single_template_call(text: str, function: str) -> TemplateCall | None:
    """Return the sole call when the entire scalar is one call of ``function``."""
    calls = scan_template_calls(text)
    if len(calls) != 1 or calls[0].function != function:
        return None
    call = calls[0]
    if text[: call.start].strip() or text[call.end :].strip():
        return None
    return call


def replace_template_calls(text: str, function: str, resolve: Callable[[TemplateCall], str]) -> str:
    """Replace calls to one function without changing any other source text."""
    calls = [call for call in scan_template_calls(text) if call.function == function]
    rendered = text
    for call in reversed(calls):
        rendered = rendered[: call.start] + resolve(call) + rendered[call.end :]
    return rendered
