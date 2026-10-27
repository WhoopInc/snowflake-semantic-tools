"""Recognize sample values shaped like personal data, whatever tags the column carries.

A column's tags decide whether enrich reads it, but a column nobody has tagged yet is read like
any other, so a value that is plainly an email address or a phone number could be written to
the YAML and published. These checks look at the value itself. They are pure and fast, and a
match is reported by its kind alone: the value is never repeated in a message.

Shapes that collide with ordinary data are gated by the column's name: an IP address only on a
column named for one, since `4.10.2.1` is also a firmware version; an unformatted run of digits
as a phone or card number only on a column named for one, since such runs are usually IDs.
"""

from __future__ import annotations

import functools
import ipaddress
import re
from collections.abc import Iterable

EMAIL = "an email address"
PHONE = "a phone number"
CARD = "a payment card number"
SSN = "a US social security number"
IP = "an IP address"

_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+(?:@|%40)[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}")
_SSN = re.compile(r"(?!000|666|9\d\d)\d{3}-(?!00)\d{2}-(?!0000)\d{4}")
_PHONE_FORMATTED = re.compile(
    r"(?:\+?1[\s.-]?)?(?:\(\d{3}\)\s?|\d{3}[\s.-])\d{3}[\s.-]\d{4}"  # (555) 123-4567, 555-123-4567
    r"|\+\d{1,3}[\s.-]?\d(?:[\s.-]?\d){7,13}"  # +44 20 7946 0958, +15551234567
)
_CARD_GROUPED = re.compile(r"\d{4}(?:[ -]\d{4}){2}[ -]\d{1,7}")
_DIGITS = re.compile(r"\d+")

_IP_WORDS = frozenset(("ip", "ipv4", "ipv6", "ipaddr", "ipaddress"))
_PHONE_WORDS = frozenset(("phone", "mobile", "cell", "telephone", "tel", "msisdn", "fax"))
_CARD_WORDS = frozenset(("card", "pan", "cc", "ccnum", "creditcard"))


@functools.lru_cache(maxsize=4096)
def _words(column_name: str) -> frozenset[str]:
    """The lower-cased words of a column name, split at underscores, punctuation, and case changes."""
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", column_name)
    words = [word for word in re.split(r"[^A-Za-z0-9]+", spaced.casefold()) if word]
    joined = {"".join(words[index : index + 2]) for index in range(len(words) - 1)}
    return frozenset((*words, *joined))


def _luhn(digits: str) -> bool:
    total = 0
    for index, character in enumerate(reversed(digits)):
        value = int(character)
        if index % 2:
            value = value * 2 - 9 if value > 4 else value * 2
        total += value
    return total % 10 == 0


def _is_ip(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return False
    return True


def pii_shape(value: str, column_name: str) -> str | None:
    """Return the kind of personal data `value` is shaped like on a column named `column_name`, or None.

    Example:
        `pii_shape("jane@example.com", "anonymous_id")` is "an email address";
        `pii_shape("4.10.2.1", "firmware_version")` is None, and on `client_ip` it is "an IP address".
    """
    text = value.strip()
    if not text:
        return None
    if ("@" in text or "%40" in text) and _EMAIL.search(text):
        return EMAIL
    digit_count = sum(character.isdigit() for character in text)
    words = _words(column_name)
    if digit_count < 9 and ":" not in text and not words & _IP_WORDS:
        return None  # every remaining shape holds at least nine digits, or is an IP address
    if digit_count == 9 and _SSN.fullmatch(text):
        return SSN
    digits = "".join(_DIGITS.findall(text))
    card_shaped = bool(_CARD_GROUPED.fullmatch(text) or (words & _CARD_WORDS and re.fullmatch(r"[\d -]+", text)))
    if card_shaped and 13 <= len(digits) <= 19 and _luhn(digits):
        return CARD
    phone_shaped = bool(
        _PHONE_FORMATTED.fullmatch(text) or (words & _PHONE_WORDS and re.fullmatch(r"[\d\s().+-]+", text))
    )
    if phone_shaped and 10 <= len(digits) <= 15:
        return PHONE
    if words & _IP_WORDS and _is_ip(text):
        return IP
    return None


def first_pii_shape(values: Iterable[str], column_name: str) -> str | None:
    """Return the kind of the first value shaped like personal data, or None when none is."""
    for value in values:
        kind = pii_shape(value, column_name)
        if kind is not None:
            return kind
    return None
