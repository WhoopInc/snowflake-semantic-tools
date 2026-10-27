"""SST-RND900: a semantic view renders a different number of members than its model carries."""

from __future__ import annotations

from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.render import semantic_view
from snowflake_semantic_tools.domain.render.semantic_view import render_checked
from tests.helpers.compile_builders import view


def test_sst_rnd900_fires(monkeypatch: pytest.MonkeyPatch) -> None:
    doubled = dict(semantic_view.MEMBER_INDEX, metric=lambda value: (*value.dimensions, *value.metrics))
    monkeypatch.setattr(semantic_view, "MEMBER_INDEX", MappingProxyType(doubled))
    ddl, [diagnostic] = render_checked(view("V"))
    assert ddl is None
    assert (diagnostic.code, diagnostic.severity) == ("SST-RND900", Severity.ERROR)
    assert diagnostic.message == "DB.SCH.V: rendered 1 members, model carries 2"


def test_sst_rnd900_silent() -> None:
    assert render_checked(view("V"))[1] == ()
