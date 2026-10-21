"""SST-VAL878: a CoWork profile publishes outside SNOWFLAKE_INTELLIGENCE.PROFILES, where CoWork lists none."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from tests.helpers.cowork_profiles import PROFILE_ORIGIN, compile_profiles, cowork_profile, described_agent
from tests.helpers.diagnostic_filters import coded


def test_sst_val878_fires() -> None:
    tree = {"cowork_profiles": {"+schema": "PROFILES_DEV"}}
    result = compile_profiles(cowork_profile(), agents=(described_agent(),), tree=tree)
    [diagnostic] = coded(result.diagnostics, "SST-VAL878")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == (
        "cowork profile 'everyone' publishes to DB.PROFILES_DEV, where CoWork does not list profiles"
    )
    assert (diagnostic.subject, diagnostic.origin) == ("cowork_profile:everyone", PROFILE_ORIGIN)


def test_sst_val878_silent() -> None:
    # The schema is compared as Snowflake resolves it, so lower case names the same one.
    tree = {"cowork_profiles": {"+database": "snowflake_intelligence", "+schema": "profiles"}}
    result = compile_profiles(cowork_profile(), agents=(described_agent(),), tree=tree)
    assert coded(result.diagnostics, "SST-VAL878") == []


def test_sst_val878_says_a_rehearsal_is_never_attached_and_keeps_its_condition() -> None:
    # Snowflake private preview (PrPr): CoWork profiles. Only the note gained the attachment.
    entry = ERROR_REGISTRY["SST-VAL878"]
    assert entry.condition == (
        "a compiled CoWork profile's database and schema, compared as Snowflake resolves them, are not "
        "SNOWFLAKE_INTELLIGENCE.PROFILES"
    )
    assert entry.note is not None and entry.note.endswith("and is never attached to CoWork.")
