"""The render loop and the contract defaults every typed compiler shares."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from snowflake_semantic_tools.app.compile import CompileResult, StandaloneArtifact, compile_each, has_error
from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin, Severity
from snowflake_semantic_tools.domain.model.lifecycle import RenderedArtifact

from .helpers import rendered


@dataclass(frozen=True, slots=True)
class Thing(StandaloneArtifact):
    label: str

    @property
    def name(self) -> str:
        return self.label

    @property
    def artifact_key(self) -> str:
        return f"thing:{self.label}"

    @property
    def artifact_type(self) -> str:
        return "thing"

    @property
    def source_files(self) -> tuple[str, ...]:
        return (f"{self.label}.yml",)

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        return rendered(self.label.upper())


def key(member: str) -> str:
    return f"thing:{member}"


def render(member: str) -> Thing:
    if member == "bad":
        raise ValueError(f"cannot render {member}")
    return Thing(member)


def error(subject: str) -> Diagnostic:
    return D("SST-VAL001", type="thing", name=subject, subject=subject)


def warning(subject: str) -> Diagnostic:
    return D("SST-VAL528", artifact=subject, subject=subject)


def test_standalone_artifact_carries_nothing_and_publishes_its_rendered_artifact_unchanged() -> None:
    item = Thing("plain")

    assert (item.member_keys, item.referenced_models, item.dbt_relations) == ((), (), ())
    assert item.rendered_for_publish("f" * 64) == item.rendered_artifact
    assert CompileResult((item,)).rendered_for_publish("0" * 64) == (item.rendered_artifact,)
    assert not hasattr(item, "__dict__")


def test_compile_each_skips_what_an_error_names_and_reports_failures_in_member_order() -> None:
    known = DiagnosticBag((error("thing:blocked"), warning("thing:warned")))

    result = compile_each(
        ["a", "blocked", "warned", "bad", "bad", "c"],
        key=key,
        render=render,
        diagnostics=known,
        origin=lambda member: Origin(f"{member}.yml", 3),
    )

    assert [item.name for item in result.compiled] == ["a", "warned", "c"]
    assert [(item.code, item.subject) for item in result.diagnostics] == [
        ("SST-VAL001", "thing:blocked"),
        ("SST-VAL528", "thing:warned"),
        # The second "bad" is skipped: the first one's SST-INT902 already names its key.
        ("SST-INT902", "thing:bad"),
    ]
    failure = result.diagnostics[-1]
    assert failure.severity is Severity.ERROR
    assert failure.origin == Origin("bad.yml", 3)
    assert failure.context["detail"] == "cannot render bad"
    assert isinstance(result.diagnostics, DiagnosticBag)


def test_compile_each_returns_the_given_diagnostics_when_nothing_fails() -> None:
    known = DiagnosticBag((warning("thing:a"),))

    result = compile_each(["a", "b"], key=key, render=render, diagnostics=known)

    assert result.diagnostics is known
    assert [item.artifact_key for item in result.compiled] == ["thing:a", "thing:b"]


def test_compile_each_without_skip_renders_everything_and_catches_only_the_given_errors() -> None:
    every = compile_each(
        ["blocked"], key=key, render=render, diagnostics=DiagnosticBag((error("thing:blocked"),)), skip=None
    )
    assert [item.name for item in every.compiled] == ["blocked"]

    def missing(member: str) -> Thing:
        raise KeyError(member)

    caught = compile_each(["a"], key=key, render=missing, diagnostics=DiagnosticBag())
    assert [(item.code, item.origin) for item in caught.diagnostics] == [("SST-INT902", None)]
    with pytest.raises(KeyError):
        compile_each(["a"], key=key, render=missing, diagnostics=DiagnosticBag(), errors=(TypeError, ValueError))


def test_custom_skip_sees_each_key_with_the_diagnostics_so_far() -> None:
    seen: list[tuple[str, int]] = []

    def skip(subject: str, diagnostics: DiagnosticBag) -> bool:
        seen.append((subject, len(diagnostics)))
        return subject == "thing:c"

    result = compile_each(["bad", "b", "c"], key=key, render=render, diagnostics=DiagnosticBag(), skip=skip)

    assert seen == [("thing:bad", 0), ("thing:b", 1), ("thing:c", 1)]
    assert [item.name for item in result.compiled] == ["b"]


def test_has_error_reads_only_error_diagnostics_that_name_the_subject() -> None:
    diagnostics = (warning("thing:x"), error("thing:y"))

    assert not has_error("thing:x", diagnostics)
    assert has_error("thing:y", diagnostics)
    assert not has_error("thing:z", diagnostics)
