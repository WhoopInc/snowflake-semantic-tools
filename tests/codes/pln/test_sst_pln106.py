"""SST-PLN106: no role in the session may attach a CoWork profile to the CoWork object.

CoWork profiles are a Snowflake private preview. Fires for a profile plan would attach when the
preflight read finds MODIFY or OWNERSHIP on the CoWork object held by no role in the session; the
same profile with a role holding it stays quiet.
"""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from tests.helpers.cowork_attach import COWORK, profile, reading
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.plan_codes import plan


def _plan(lacking: tuple[str, ...]) -> tuple[Action, DiagnosticBag]:
    preflight = Preflight("dev", "DEPLOYER", missing_cowork_privileges=MappingProxyType({COWORK: lacking}))
    [change] = (changeset := plan((profile(),), preflight=preflight, attachment=reading())).changes
    return change.action, changeset.diagnostics


def test_sst_pln106_fires() -> None:
    action, diagnostics = _plan(("MODIFY",))
    diagnostic = only(diagnostics, "SST-PLN106")
    assert (action, diagnostic.severity, diagnostic.subject) == (
        Action.BLOCKED,
        Severity.ERROR,
        "cowork_profile:everyone",
    )
    assert diagnostic.message == (
        "cowork_profile:everyone: no role in the session holds MODIFY or OWNERSHIP on the CoWork object "
        "SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT"
    )


def test_sst_pln106_silent() -> None:
    action, diagnostics = _plan(())
    assert action is Action.CREATE and "SST-PLN106" not in codes(diagnostics)
