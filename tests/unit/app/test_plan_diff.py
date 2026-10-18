"""Plan's member diff: each planned update's live definition read and compared with what SST renders."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from snowflake_semantic_tools.app.compile import CompiledView
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.plan_diff import diff_updates
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, Change, ChangeReason
from snowflake_semantic_tools.domain.model.semantic_view import Column, ColumnKind, Metric, SemanticView, Table
from snowflake_semantic_tools.domain.plan.member_diff import CHANGED, MemberDiff, PropertyDiff
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.plan_codes import live
from tests.helpers.plan_codes import view as rendered_view
from tests.helpers.semantic_view_rows import described_view
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import authored, statement


def _model(comment: str = "Orders placed.") -> SemanticView:
    return SemanticView(
        fqn="DB.SCH.SALES",
        tables=(Table("ORDERS", "DB.MARTS.ORDERS", primary_key=("ORDER_ID",)),),
        columns=(Column("ORDERS", "ORDER_ID", ColumnKind.DIMENSION, authored("ORDERS.ORDER_ID")),),
        metrics=(Metric("ORDER_COUNT", authored("COUNT(ORDERS.ORDER_ID)"), "ORDERS", comment=comment),),
    )


def _change(action: Action = Action.UPDATE, name: str = "sales") -> Change:
    return Change(
        key=f"semantic_view:{name}",
        artifact_type="semantic_view",
        action=action,
        reason=ChangeReason.FINGERPRINT_DIFFERS,
        rendered=rendered_view(name),
        observed=None,
        depends_on=(),
        order=0,
    )


def _compiled(model: SemanticView) -> dict[str, CompiledView]:
    item = CompiledView(model, statement(f"CREATE SEMANTIC VIEW {model.fqn}"))
    return {item.artifact_key: item}


def test_an_update_carries_what_it_changes_member_by_member() -> None:
    port = FakeSnowflake()
    port.view_descriptions["DB.SCH.SALES"] = described_view(_model("Orders."))
    [change], refused = diff_updates((_change(),), _compiled(_model()), Fanout(port))
    assert refused == ()
    assert change.member_diff == (
        MemberDiff("metric", "orders.order_count", CHANGED, (PropertyDiff("comment", "Orders.", "Orders placed."),)),
    )


def test_an_update_whose_members_all_agree_carries_an_empty_diff() -> None:
    port = FakeSnowflake()
    port.view_descriptions["DB.SCH.SALES"] = described_view(_model())
    [change], _ = diff_updates((_change(),), _compiled(_model()), Fanout(port))
    assert change.member_diff == ()


def test_the_object_observed_is_the_one_read() -> None:
    port = FakeSnowflake()
    port.view_descriptions["DB.SCH.OLD_SALES"] = described_view(_model())
    # A renamed view: the update replaces the object observed, not the one it now names.
    observed = live(rendered_view("old_sales"))
    [change], refused = diff_updates((replace(_change(), observed=observed),), _compiled(_model()), Fanout(port))
    assert (change.member_diff, refused) == ((), ())


def test_only_updates_of_a_comparable_type_with_a_compiled_artifact_are_read() -> None:
    port = FakeSnowflake()
    changes = (
        _change(Action.CREATE),
        _change(Action.NOOP),
        replace(_change(), artifact_type="search_service"),
        _change(name="menu"),
    )
    assert diff_updates(changes, _compiled(_model()), Fanout(port)) == (changes, ())


def test_a_refused_read_leaves_the_update_uncompared_and_says_why() -> None:
    port = FakeSnowflake()
    port.fail("describe_semantic_view", SnowflakePortError("insufficient privileges"))
    [change], [diagnostic] = diff_updates((_change(),), _compiled(_model()), Fanout(port))
    assert change.member_diff is None
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-PLN041",
        Severity.INFO,
        "semantic_view:sales",
    )


def test_a_compile_and_change_that_do_not_fit_are_a_caller_bug() -> None:
    port = FakeSnowflake()
    port.view_descriptions["DB.SCH.SALES"] = described_view(_model())
    with pytest.raises(TypeError, match="did not compile as a semantic view"):
        stand_in = SimpleNamespace(artifact_key="semantic_view:sales")
        diff_updates((_change(),), {"semantic_view:sales": stand_in}, Fanout(port))
    with pytest.raises(ValueError, match="names no object"):
        diff_updates((replace(_change(), rendered=None),), _compiled(_model()), Fanout(port))
