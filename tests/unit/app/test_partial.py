"""`--partial` publishes what has no errors and depends on nothing that does."""

from __future__ import annotations

from dataclasses import dataclass

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.partial import partial_refusal, partial_split
from snowflake_semantic_tools.domain.model.diagnostic import D, DiagnosticBag


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
    metric_error = D("SST-REF038", subject="metric:total_revenue", artifact="metric:total_revenue", name="nope")
    broken = result(metric_error)
    assert partial_split(broken) is None
    refusal = partial_refusal(broken)
    assert refusal is not None and refusal.code == "SST-PLN033"
    assert refusal.message.startswith("--partial publishes nothing: SST-REF038 on metric:total_revenue")
    assert partial_refusal(result()) is None
