"""`--partial` publishes what has no errors and depends on nothing that does."""

from __future__ import annotations

from dataclasses import dataclass, replace

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.partial import partial_changeset, partial_refusal, partial_split
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.lifecycle import Action, Change, ChangeReason
from snowflake_semantic_tools.domain.plan.changeset import narrowed as narrowed_plan
from tests.helpers.artifact_builders import change, changeset, rendered


@dataclass(frozen=True)
class Rendered:
    depends_on: tuple[str, ...]


@dataclass(frozen=True)
class Artifact:
    artifact_key: str
    depends_on: tuple[str, ...] = ()

    @property
    def rendered_artifact(self) -> Rendered:
        return Rendered(self.depends_on)


def result(*diagnostics: object) -> CompileResult:
    compiled = (
        Artifact("semantic_view:sales"),
        Artifact("semantic_view:menu"),
        Artifact("agent:analyst", ("semantic_view:menu", "semantic_view:sales")),
        Artifact("eval:analyst", ("agent:analyst",)),
    )
    return CompileResult(compiled, DiagnosticBag(diagnostics))  # type: ignore[arg-type]


def test_an_error_excludes_its_artifact_and_everything_that_depends_on_it() -> None:
    split = partial_split(result(D("SST-REF044", subject="semantic_view:MENU", artifact="menu", found="x")))
    assert split is not None
    assert [item.artifact_key for item in split.healthy.compiled] == ["semantic_view:sales"]
    assert split.excluded == ("agent:analyst", "eval:analyst", "semantic_view:MENU")
    assert [item.code for item in split.notices] == ["SST-PLN032"] * 3
    assert split.healthy.diagnostics.has_errors


def test_an_error_on_something_never_compiled_still_excludes_its_dependents() -> None:
    split = partial_split(result(D("SST-VAL855", subject="skill:gone", artifact="p", kind="skill", name="gone")))
    assert split is not None and len(split.healthy.compiled) == 4
    assert split.excluded == ("skill:gone",)


def test_an_error_that_names_no_artifact_leaves_nothing_to_publish() -> None:
    assert partial_split(result(D("SST-CFG047", subject="config:project.hooks_dir", key="k", value="v"))) is None
    assert partial_split(result(D("SST-INT902", detail="x"))) is None


def test_without_errors_everything_is_healthy() -> None:
    split = partial_split(result(D("SST-VAL804", subject="skill:x", artifact="skill:x", value="SST_1")))
    assert split is not None and len(split.healthy.compiled) == 4 and split.excluded == ()


@dataclass(frozen=True)
class Container(Artifact):
    contained_keys: tuple[str, ...] = ()


def test_an_error_in_something_an_artifact_carries_keeps_the_artifact_back() -> None:
    compiled = (
        Container("profile:analyst", contained_keys=("profile:shared", "skill:a", "command:daily")),
        Container("profile:operator", contained_keys=("profile:shared", "plugin:kit")),
        Container("plugin:kit", contained_keys=("skill:b",)),
        Artifact("skill:b"),
    )
    command_error = D("SST-VAL859", subject="command:daily", artifact="daily", detail="bad")
    split = partial_split(CompileResult(compiled, DiagnosticBag((command_error,))))  # type: ignore[arg-type]
    assert split is not None
    assert [item.artifact_key for item in split.healthy.compiled] == ["profile:operator", "plugin:kit", "skill:b"]
    assert split.excluded == ("command:daily", "profile:analyst")

    member_error = D("SST-VAL808", subject="skill:b", artifact="b", path="x.md")
    split = partial_split(CompileResult(compiled, DiagnosticBag((member_error,))))  # type: ignore[arg-type]
    assert split is not None and [item.artifact_key for item in split.healthy.compiled] == ["profile:analyst"]


def test_a_semantic_view_member_error_stops_the_split_and_says_why() -> None:
    metric_error = D("SST-CFG029", subject="metric:total_revenue", var="nope")
    broken = result(metric_error)
    assert partial_split(broken) is None
    refusal = partial_refusal(broken)
    assert refusal is not None and refusal.code == "SST-PLN033"
    assert refusal.message.startswith("--partial publishes nothing: SST-CFG029 on metric:total_revenue")
    assert partial_refusal(result()) is None


def test_an_error_on_a_tool_members_reference_excludes_its_tool_and_the_agent_using_it() -> None:
    compiled = (
        Artifact("semantic_view:sales"),
        Artifact("tool:delivery_faq_search"),
        Artifact("agent:concierge", ("tool:delivery_faq_search",)),
    )
    unmapped = D("SST-REF018", ref_function="tool", name="delivery_faq_search", target="dev")
    error = replace(unmapped, subject="tool_ref:delivery_faq_search")
    split = partial_split(CompileResult(compiled, DiagnosticBag((error,))))  # type: ignore[arg-type]
    assert split is not None
    assert [item.artifact_key for item in split.healthy.compiled] == ["semantic_view:sales"]
    assert split.excluded == ("agent:concierge", "tool:delivery_faq_search")
    assert partial_refusal(CompileResult(compiled, DiagnosticBag((error,)))) is None  # type: ignore[arg-type]


def _blocked(name: str, *diagnostics: Diagnostic, depends_on: tuple[str, ...] = ()) -> Change:
    artifact = rendered(name, depends_on=depends_on)
    reason = ChangeReason.VALIDATION_ERRORS if diagnostics else ChangeReason.DEPENDENCY_BLOCKED
    return replace(
        change(artifact, Action.UPDATE), action=Action.BLOCKED, reason=reason, diagnostics=DiagnosticBag(diagnostics)
    )


def test_a_partial_plan_leaves_out_what_it_blocked_against_the_target_and_keeps_the_rest() -> None:
    out_of_band = D("SST-PLN014", artifact="semantic_view:people")
    unmanaged = D("SST-PLN024", artifact="semantic_view:orphan", value="DB.SCHEMA.ORPHAN")
    moved = D("SST-PLN025", artifact="semantic_view:moved", found="DB.OLD.MOVED", expected="DB.SCHEMA.MOVED")
    finance = change(rendered("FINANCE"), Action.UPDATE)
    plan = replace(
        changeset(
            _blocked("PEOPLE", out_of_band),
            _blocked("ORPHAN", unmanaged),
            _blocked("MOVED", moved),
            # Blocked because it depends on PEOPLE, which reports nothing of its own.
            _blocked("REPORT", depends_on=("semantic_view:people",)),
            finance,
        ),
        diagnostics=DiagnosticBag((out_of_band, unmanaged, moved)),
    )

    narrowed, left_out = partial_changeset(plan)

    assert [item.key for item in narrowed.changes] == [finance.key]
    assert narrowed.blocked == () and not narrowed.diagnostics.has_errors
    assert narrowed.plan_id == narrowed_plan(plan, (finance,), DiagnosticBag()).plan_id != ""
    assert [item.code for item in left_out] == ["SST-PLN014", "SST-PLN024", "SST-PLN025", *["SST-PLN032"] * 4]
    assert [item.subject for item in left_out if item.code == "SST-PLN032"] == [
        "semantic_view:people",
        "semantic_view:orphan",
        "semantic_view:moved",
        "semantic_view:report",
    ]
    # The errors stay among what the run left out, so it still exits 1.
    assert DiagnosticBag(left_out).has_errors


def test_a_plan_with_nothing_blocked_is_left_as_it_is() -> None:
    plan = changeset(change(rendered("FINANCE"), Action.UPDATE))
    assert partial_changeset(plan) == (plan, ())


def test_a_blocked_changes_own_diagnostic_the_plan_did_not_list_is_still_kept() -> None:
    own = D("SST-PLN014", artifact="semantic_view:people")
    narrowed, left_out = partial_changeset(changeset(_blocked("PEOPLE", own)))
    assert narrowed.changes == () and [item.code for item in left_out] == ["SST-PLN014", "SST-PLN032"]
