"""SST-PLN104: an unchanged CoWork profile is no longer attached to the CoWork object.

CoWork profiles are a Snowflake private preview. Fires for a recorded, unchanged profile that SHOW
AGENTS IN SNOWFLAKE INTELLIGENCE does not list; the same profile listed there stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, DiagnosticBag, Severity
from snowflake_semantic_tools.domain.plan.attach import AttachmentReading
from tests.helpers.cowork_attach import MANIFEST, live, profile, reading
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.plan_codes import entry, manifest_of, plan


def _plan(attachment: AttachmentReading) -> DiagnosticBag:
    artifact = profile()
    return plan(
        (artifact,),
        observed=(live(artifact),),
        applied={artifact.key: entry(artifact, MANIFEST)},
        manifest=manifest_of(artifact),
        attachment=attachment,
    ).diagnostics


def test_sst_pln104_fires() -> None:
    diagnostic = only(_plan(reading()), "SST-PLN104")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.subject == "cowork_profile:everyone"
    assert diagnostic.message == (
        "cowork_profile:everyone: not attached to the CoWork object SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT; "
        "apply attaches it again"
    )
    # A hand detach never fails a strict run: the apply it plans is the repair.
    assert ERROR_REGISTRY["SST-PLN104"].strict_exempt


def test_sst_pln104_silent() -> None:
    assert "SST-PLN104" not in codes(_plan(reading(profile())))
