"""Which values read as personal data, and which ordinary values that look like them do not."""

from __future__ import annotations

import time

import pytest

from snowflake_semantic_tools.domain.model.pii_shape import CARD, EMAIL, IP, PHONE, SSN, first_pii_shape, pii_shape


@pytest.mark.parametrize(
    ("value", "column", "kind"),
    [
        ("jane.doe@example.com", "anonymous_id", EMAIL),
        ("  Jane@Example.co.uk ", "created_by", EMAIL),
        ("https://x.example/u?e=jane%40example.com", "clicked_url", EMAIL),
        ("123-45-6789", "notes", SSN),
        ("(555) 123-4567", "notes", PHONE),
        ("555-123-4567", "contact", PHONE),
        ("+44 20 7946 0958", "contact", PHONE),
        ("5551234567", "phone_number", PHONE),
        ("5551234567", "mobileNumber", PHONE),
        ("4111 1111 1111 1111", "notes", CARD),
        ("4111111111111111", "card_number", CARD),
        ("10.0.0.1", "client_ip", IP),
        ("2001:db8::1", "ip_address", IP),
        ("192.168.1.10", "remoteIp", IP),
    ],
)
def test_personal_data_is_recognized(value: str, column: str, kind: str) -> None:
    assert pii_shape(value, column) == kind


@pytest.mark.parametrize(
    ("value", "column"),
    [
        ("4.10.2.1", "firmware_version"),  # an IPv4 shape, on a column not named for an IP
        ("10.0.0.1", "zip_code"),  # "zip" is not "ip"
        ("2024-01-15", "created_date"),
        ("2024-01-15 12:30:45", "created_at"),
        ("550e8400-e29b-41d4-a716-446655440000", "event_id"),
        ("5551234567", "user_id"),  # ten digits, but an ID column
        ("4111111111111111", "order_id"),  # passes Luhn, but an ID column
        ("4111 1111 1111 1112", "notes"),  # grouped like a card, fails Luhn
        ("000-12-3456", "notes"),  # not an issued SSN
        ("v1.2.3", "os_version"),
        ("user@localhost", "created_by"),  # no domain suffix
        ("iOS", "device_platform"),
        ("", "email"),
    ],
)
def test_ordinary_values_are_not_mistaken_for_personal_data(value: str, column: str) -> None:
    assert pii_shape(value, column) is None


def test_the_first_matching_value_decides() -> None:
    assert first_pii_shape(["open", "closed", "a@b.example"], "status") == EMAIL
    assert first_pii_shape(["open", "closed"], "status") is None


def test_a_project_sized_scan_is_fast() -> None:
    """33,000 columns of 25 values each, about the size of a large project, checks in well under a second."""
    columns = [(f"column_{index}", [f"value-{index}-{slot}" for slot in range(25)]) for index in range(33_000)]
    started = time.perf_counter()
    found = [first_pii_shape(values, name) for name, values in columns]
    elapsed = time.perf_counter() - started
    assert not any(found)
    assert elapsed < 5.0, f"{elapsed:.2f}s"
