"""The NUL placeholder mark: only `sql()` writes it, so no template or sealed text may hold one."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.sql import sql
from snowflake_semantic_tools.domain.sql.core import _seal


def test_a_template_holding_the_placeholder_mark_is_refused() -> None:
    with pytest.raises(ValueError, match="may not hold a NUL character"):
        sql("SELECT 1 \x00")


def test_sealed_text_holding_the_placeholder_mark_is_refused() -> None:
    with pytest.raises(ValueError, match="may not hold a NUL character"):
        _seal("SELECT \x00")
    assert _seal("SELECT 1").text == "SELECT 1"
