"""A finding is framed at the source line it names, and a run ends with one verdict line."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from snowflake_semantic_tools.cli.render.diagnostics import (
    FindingNotes,
    Verdict,
    location,
    plural,
    render_collapsed,
    render_finding,
    render_verdict,
    sorted_for_reading,
)
from snowflake_semantic_tools.cli.render.frames import Excerpt, _fitted, display_width, excerpt, pinpoint
from snowflake_semantic_tools.cli.render.next_steps import default_next_steps
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.render.terminal import PLAIN, Terminal
from snowflake_semantic_tools.domain.diagnostics import D, Origin, Severity

METRIC = """metric:
  name: customer_count
  description: Number of customers.
  expr: "COUNT(DISTINCT {{ ref('customers', 'customer_uuid') }})"
other:
  name: customer_uuid
"""


def _project(tmp_path: Path, text: str = METRIC, name: str = "metric.yml") -> Path:
    (tmp_path / name).write_text(text)
    return tmp_path


def test_the_caret_covers_the_token_at_the_column(tmp_path: Path) -> None:
    found = excerpt(_project(tmp_path), Origin("metric.yml", 2, 3), 100)
    assert found == Excerpt(2, "  name: customer_count", 2, 4)


LINE = "  expr: 'abc' x {{ ref('customers', 'id') }} ab"


@pytest.mark.parametrize(
    ("token", "length"),
    [("'abc'", 5), ("{{", len("{{ ref('customers', 'id') }}")), ("ab\n", 2)],
)
def test_a_token_is_a_quoted_string_a_template_call_or_a_bare_run(tmp_path: Path, token: str, length: int) -> None:
    column = (LINE + "\n").index(token, 10 if token == "ab\n" else 0) + 1
    found = excerpt(_project(tmp_path, LINE + "\n"), Origin("metric.yml", 1, column), 100)
    assert found is not None and found.length == length


def test_an_origin_without_a_column_marks_the_line_from_its_indent(tmp_path: Path) -> None:
    found = excerpt(_project(tmp_path), Origin("metric.yml", 3, None), 100)
    assert found is not None and found.start == 2 and found.length == len("description: Number of customers.")


@pytest.mark.parametrize("line", [None, 0, 99])
def test_no_excerpt_without_a_line_the_file_has(tmp_path: Path, line: int | None) -> None:
    assert excerpt(_project(tmp_path), Origin("metric.yml", line, 1), 100) is None


def test_no_excerpt_for_a_missing_file(tmp_path: Path) -> None:
    assert excerpt(tmp_path, Origin("absent.yml", 1, 1), 100) is None


def test_control_characters_never_reach_the_terminal(tmp_path: Path) -> None:
    found = excerpt(_project(tmp_path, "a: \x1b[31mred\tx\n"), Origin("metric.yml", 1, 1), 100)
    assert found is not None and "\x1b" not in found.text and "\t" not in found.text


def test_a_column_past_the_end_marks_the_end(tmp_path: Path) -> None:
    found = excerpt(_project(tmp_path, "ab\n"), Origin("metric.yml", 1, 40), 100)
    assert found is not None and found.start == 2 and found.length == 1


def test_a_wide_line_is_cut_to_a_window_around_the_caret() -> None:
    text = "x" * 200 + " target " + "y" * 200
    fitted = _fitted(Excerpt(1, text, 201, 6), 60)
    assert fitted.text.startswith("...") and fitted.text.endswith("...")
    assert fitted.text[fitted.start : fitted.start + 6] == "target"
    assert display_width(fitted.text) <= 60


def test_a_narrow_line_is_left_whole() -> None:
    found = Excerpt(1, "short", 0, 5)
    assert _fitted(found, 60) is found


def test_wide_characters_take_two_columns() -> None:
    assert display_width("ab") == 2 and display_width("日本") == 4


def test_pinpoint_moves_to_the_value_a_hint_names(tmp_path: Path) -> None:
    moved = pinpoint(_project(tmp_path), Origin("metric.yml", 2, 3), ["customer_uuid"])
    assert (moved.line, moved.col) == (4, METRIC.splitlines()[3].index("customer_uuid") + 1)


def test_pinpoint_stays_inside_the_origins_block(tmp_path: Path) -> None:
    project = _project(tmp_path, "metric:\n  name: a\nother:\n  name: customer_uuid\n")
    origin = Origin("metric.yml", 2, 3)
    assert pinpoint(project, origin, ["customer_uuid"]) == origin


def test_pinpoint_finds_a_dotted_hint_by_its_last_part(tmp_path: Path) -> None:
    project = _project(tmp_path, "view:\n  name: v\n  exclude:\n    - \"{{ ref('orders', 'tax_payed') }}\"\n")
    moved = pinpoint(project, Origin("metric.yml", 2, 3), ["orders.tax_payed"])
    assert moved.line == 4


def test_pinpoint_finds_the_names_a_template_hint_quotes(tmp_path: Path) -> None:
    project = _project(tmp_path, "metrics:\n  - name: order_count\n    expr: \"{{ ref('missing', 'order_id') }}\"\n")
    moved = pinpoint(project, Origin("metric.yml", 2, 5), ["{{ ref('missing') }}"])
    assert (moved.line, moved.col) == (
        3,
        project.joinpath("metric.yml").read_text().splitlines()[2].index("missing") + 1,
    )


def test_pinpoint_skips_a_hint_inside_a_longer_name(tmp_path: Path) -> None:
    project = _project(tmp_path, "m:\n  name: customer_uuid_x\n  expr: customer_uuid\n")
    moved = pinpoint(project, Origin("metric.yml", 2, 3), ["customer_uuid"])
    assert moved.line == 3


@pytest.mark.parametrize("hints", [[], ["x"], ["absent_value"]])
def test_pinpoint_leaves_an_origin_it_cannot_improve(tmp_path: Path, hints: list[str]) -> None:
    origin = Origin("metric.yml", 2, 3)
    assert pinpoint(_project(tmp_path), origin, hints) == origin
    assert pinpoint(tmp_path, Origin("absent.yml", 2, 3), ["customer_uuid"]).file == "absent.yml"


def test_a_finding_is_located_framed_and_followed_by_what_to_do(tmp_path: Path) -> None:
    diagnostic = D("SST-REF002", origin=Origin("metric.yml", 2, 3), column="customer_uuid", model="customers")
    text = render_finding(diagnostic, Style(PLAIN), _project(tmp_path), FindingNotes(hidden_cascades=2))
    lines = text.splitlines()
    assert lines[0].startswith("error[SST-REF002]: ")
    assert lines[1] == " --> metric.yml:4:" + str(METRIC.splitlines()[3].index("customer_uuid") + 1)
    assert lines[3].startswith("4 |   expr:")
    assert lines[4].endswith("^" * len("customer_uuid"))
    assert "  = help: " in text and "  = sst explain SST-REF002" in text
    assert "  = 2 follow-on findings hidden (--show-cascade)" in text


def test_a_finding_without_a_file_has_no_frame() -> None:
    diagnostic = D("SST-REF002", column="customer_uuid", model="customers")
    text = render_finding(diagnostic, Style(PLAIN), Path("."), FindingNotes(fingerprint="abc"))
    assert "-->" not in text and "|" not in text
    assert "fingerprint: abc" in text


def test_a_finding_discloses_a_promotion_and_the_baseline() -> None:
    warning = D("SST-PRS004", artifact="semantic_view:v", field="f")
    diagnostic = dataclasses.replace(warning, severity=Severity.ERROR, declared=Severity.WARNING)
    text = render_finding(diagnostic, Style(PLAIN), Path("."), FindingNotes(baselined=True))
    assert text.startswith("error[SST-PRS004] (promoted from warning, baselined): ")


def test_a_finding_lists_the_related_places() -> None:
    related = tuple(Origin(f"f{index}.yml", index, 1) for index in range(1, 6))
    diagnostic = D("SST-REF002", column="c", model="m", related=related)
    text = render_finding(diagnostic, Style(PLAIN), Path("."), FindingNotes())
    assert "= also at: f1.yml:1:1, f2.yml:2:1, f3.yml:3:1 and 2 more" in text


def test_a_message_repeating_its_location_is_not_located_twice() -> None:
    diagnostic = D("SST-LOD005", file="m.yml", line=2, key="name")
    text = render_finding(diagnostic, Style(PLAIN), Path("."), FindingNotes())
    assert text.splitlines()[0].endswith(": duplicate key 'name'")
    assert "m.yml:2: duplicate" not in text


def test_the_location_is_cut_to_what_the_origin_knows() -> None:
    assert location(Origin("a.yml", None, None)) == "a.yml"
    assert location(Origin("a.yml", 3, None)) == "a.yml:3"
    assert location(Origin("a.yml", 3, 4)) == "a.yml:3:4"


def test_plural_keeps_info_as_it_is() -> None:
    assert (plural(1, "error"), plural(2, "error"), plural(3, "info")) == ("1 error", "2 errors", "3 info")


def test_findings_read_errors_first_in_report_order() -> None:
    warning = D("SST-PRS004", artifact="a", field="f")
    first, second = (D("SST-REF002", column=name, model="m") for name in ("a", "b"))
    assert sorted_for_reading([warning, first, second]) == [first, second, warning]


def test_a_collapsed_group_names_the_flag_that_shows_it() -> None:
    assert render_collapsed("SST-RND013", 4, Style(PLAIN)) == "  ... and 4 more SST-RND013 (--show-all-occurrences)"


def test_a_failed_verdict_counts_hides_and_suggests() -> None:
    verdict = Verdict(
        "fail",
        "validate failed",
        errors=2,
        warnings=1,
        baselined=8,
        hidden={"info": 166, "cascade": 1, "baselined": 0},
        seconds=1.12,
        next_step="fix the errors, then run `sst validate` again",
    )
    assert render_verdict(verdict, Style(Terminal(unicode=False))).splitlines() == [
        "x validate failed: 2 errors, 1 warning - 8 baselined - 1.1s",
        "  hidden: 166 info (--show-info), 1 cascade (--show-cascade)",
        "  next: fix the errors, then run `sst validate` again",
    ]


def test_a_passing_verdict_reads_as_its_headline() -> None:
    style = Style(Terminal())
    assert render_verdict(Verdict("ok", "validated 29 artifacts", errors=0, seconds=75.0), style) == (
        "✓ validated 29 artifacts · 0 errors, 0 warnings · 1m15s"
    )
    assert render_verdict(Verdict("changes", "plan: 1 to create", note="agent detected"), style) == (
        "~ plan: 1 to create (agent detected)"
    )


@pytest.mark.parametrize(
    ("command", "exit_code", "errors", "expected"),
    [
        ("validate", 3, 0, "see `sst validate --help`"),
        ("plan", 4, 1, "fix the setup problem"),
        ("plan", 5, 1, "`sst debug`"),
        ("apply", 130, 0, "`sst plan`"),
        ("validate", 1, 2, "fix the errors, then run `sst validate` again"),
        ("validate", 1, 1, "fix the error, then"),
        ("plan", 2, 0, "only when you mean to"),
        ("validate", 0, 0, "`sst plan` shows"),
    ],
)
def test_the_default_next_step_follows_how_the_run_ended(
    command: str, exit_code: int, errors: int, expected: str
) -> None:
    steps = default_next_steps(command, exit_code, errors=errors, warnings=0)
    assert steps and expected in steps[0]


@pytest.mark.parametrize(
    ("command", "exit_code", "warnings"), [("compile", 0, 0), ("validate", 0, 2), ("test", 130, 0)]
)
def test_no_next_step_where_there_is_nothing_to_do(command: str, exit_code: int, warnings: int) -> None:
    assert default_next_steps(command, exit_code, errors=0, warnings=warnings) == ()
