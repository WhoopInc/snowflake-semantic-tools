"""The comparison settings an eval resolves: its candidate models and its runs per model."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.eval import eval_defaults
from snowflake_semantic_tools.domain.validate.eval_config import candidate_model_problems, eval_candidate_problems
from tests.helpers.diagnostic_filters import codes
from tests.helpers.eval_inputs import SETTINGS, sales_eval, settings_of, validate


def test_candidates_keep_each_named_model_once_in_the_order_written() -> None:
    settings = settings_of({"candidate_models": ["claude-opus-4-8", "", 3, "Claude-Opus-4-8", "gpt-5"]})
    assert eval_defaults(settings).candidate_models == ("claude-opus-4-8", "gpt-5")


def test_candidates_that_are_not_a_list_read_as_none() -> None:
    assert eval_defaults(settings_of({"candidate_models": "claude-opus-4-8"})).candidate_models == ()
    assert eval_defaults(settings_of({})).candidate_models == ()


def test_compare_runs_reads_an_integer_and_nothing_else() -> None:
    assert eval_defaults(settings_of({"compare_runs": 3})).compare_runs == 3
    assert eval_defaults(settings_of({"compare_runs": True})).compare_runs is None


def test_compare_runs_below_one_is_refused_where_it_was_written() -> None:
    found = validate(sales_eval(settings={**SETTINGS, "compare_runs": 0}))
    assert "SST-PRS016" in codes(found)
    assert any("config.compare_runs" in item.message for item in found if item.code == "SST-PRS016")


def test_every_bad_candidate_is_reported_in_list_order() -> None:
    found = candidate_model_problems(
        ("gpt-5", " auto ", "claude-sonnet-4-6"),
        frozenset({"claude-sonnet-4-6"}),
        artifact="sales",
        field="--compare-models",
        subject="eval:sales",
    )
    assert [item.code for item in found] == ["SST-VAL771", "SST-VAL770"]
    assert found[1].message == "eval 'sales': --compare-models names `auto`, which is no model a comparison can run"


def test_an_evals_candidates_are_checked_where_the_setting_was_written() -> None:
    resolved = sales_eval(settings={**SETTINGS, "candidate_models": ["auto", "gpt-5"]})
    found = eval_candidate_problems(resolved, frozenset({"claude-sonnet-4-6"}))
    assert [(item.code, item.context["field"]) for item in found] == [
        ("SST-VAL770", "config.candidate_models"),
        ("SST-VAL771", "config.candidate_models"),
    ]
    assert eval_candidate_problems(sales_eval(), frozenset()) == ()
