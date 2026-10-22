"""The comparison's precheck: a subject must be published exactly as compiled, served from its release."""

from __future__ import annotations

from dataclasses import replace

import pytest

from snowflake_semantic_tools.app.evals.compare_precheck import published_release
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.compare_world import RELEASE, compare_subject, publish
from tests.helpers.snowflake_fake import FakeSnowflake


def refusal(found: str | Diagnostic) -> str:
    assert isinstance(found, Diagnostic) and found.code == "SST-APL108", found
    return found.message.removeprefix(
        "agent:sales_agent: the model comparison needs it published exactly as compiled, and "
    )


def test_a_subject_published_as_compiled_runs_its_pinned_model_on_the_release() -> None:
    port = FakeSnowflake()
    subject = compare_subject()
    assert published_release(port, subject, publish(port, subject), fetched_at="now") == RELEASE


def test_a_subject_state_records_no_apply_of_is_refused() -> None:
    port = FakeSnowflake()
    subject = compare_subject()
    entry = publish(port, subject)
    assert refusal(published_release(port, subject, None, fetched_at="now")) == (
        "state records no apply of it on this target"
    )
    failed = replace(entry, outcome="failed_after_write")
    assert refusal(published_release(port, subject, failed, fetched_at="now")).startswith("state records no apply")


def test_a_subject_that_cannot_be_read_is_refused() -> None:
    port = FakeSnowflake()
    subject = compare_subject()
    entry = publish(port, subject)
    port.refuse("show_objects")
    assert refusal(published_release(port, subject, entry, fetched_at="now")).startswith("reading it failed: ")


def test_a_subject_a_plan_would_change_is_refused_naming_the_change() -> None:
    port = FakeSnowflake()
    published = compare_subject()
    entry = publish(port, published)
    # The project changed since the apply: it now renders another definition.
    edited = replace(published, rendered=replace(published.rendered, fingerprint="e" * 64))
    assert refusal(published_release(port, edited, entry, fetched_at="now")) == (
        "`sst plan` shows it as update (fingerprint_differs)"
    )
    moved: AppliedEntry = replace(entry, qualified_name="DB.S.ELSEWHERE")
    assert refusal(published_release(port, published, moved, fetched_at="now")) == (
        "`sst plan` shows it as blocked (target_moved)"
    )


def test_a_subject_without_a_live_version_is_one_a_plan_would_update() -> None:
    port = FakeSnowflake()
    subject = compare_subject()
    entry = publish(port, subject)
    rows = port.agent_version_rows[subject.target.sql]
    port.agent_version_rows[subject.target.sql] = rows[1:]
    assert (
        refusal(published_release(port, subject, entry, fetched_at="now"))
        == "`sst plan` shows it as update (version_drift)"
    )


@pytest.mark.parametrize(
    ("setting", "detail"),
    [
        (None, "its default was not read"),
        ("LAST", "`sst plan` shows it as update (default_unpinned)"),
    ],
)
def test_a_default_not_pinned_by_name_to_the_release_is_refused(setting: str | None, detail: str) -> None:
    port = FakeSnowflake()
    subject = compare_subject()
    entry = publish(port, subject)
    if setting is None:
        del port.agent_default_settings[subject.target.sql]
    else:
        port.agent_default_settings[subject.target.sql] = setting
    assert refusal(published_release(port, subject, entry, fetched_at="now")).startswith(detail)
