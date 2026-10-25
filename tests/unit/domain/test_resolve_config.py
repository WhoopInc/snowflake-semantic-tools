"""The two template forms the configuration file evaluates: a target conditional, and `var()`."""

from __future__ import annotations

from snowflake_semantic_tools.domain.resolve.config import has_target_conditional, render_config

CONDITIONAL = "{{ 'PROD_DB' if target.name == 'prod' else 'DEV_DB' if target.name != 'ci' else 'CI_DB' }}"


def test_a_conditional_anywhere_outside_vars_needs_the_target_name() -> None:
    assert has_target_conditional({"catalog": {"+database": CONDITIONAL}})
    assert not has_target_conditional({"vars": {"db": CONDITIONAL}, "list": [CONDITIONAL], "n": 1})


def test_a_conditional_resolves_to_its_first_true_arm_or_its_final_else() -> None:
    tree = {"catalog": {"+database": CONDITIONAL}, "vars": {"db": CONDITIONAL}}
    for target, expected in (("prod", "PROD_DB"), ("dev", "DEV_DB"), ("ci", "CI_DB")):
        resolved, diagnostics = render_config(tree, target_name=target, file="sst_config.yml")
        assert resolved["catalog"]["+database"] == expected and diagnostics == ()
        # vars: is left as written.
        assert resolved["vars"] == {"db": CONDITIONAL}


def test_without_a_target_name_a_conditional_is_left_as_written() -> None:
    resolved, diagnostics = render_config({"a": CONDITIONAL}, target_name=None, file="sst_config.yml")
    assert (resolved["a"], diagnostics) == (CONDITIONAL, ())


def test_in_and_not_in_test_membership_of_a_tuple_or_list() -> None:
    # The form analytics-dbt first wrote for its production targets, which resolved to nothing.
    production = "{{ 'SST' if target.name in ('prod', 'ci_prod', 'prefect_prod') else '' }}"
    for target, expected in (("prod", "SST"), ("prefect_prod", "SST"), ("dev", "")):
        resolved, diagnostics = render_config({"state": {"+database": production}}, target_name=target, file="f")
        assert (resolved["state"]["+database"], diagnostics) == (expected, ())
    spellings = (
        "{{ 'A' if target.name in ['prod'] else 'B' }}",
        "{{ 'A' if target.name in ('prod',) else 'B' }}",
        "{{ 'A' if target.name in( 'prod' , 'qa' , ) else 'B' }}",
        "{{ 'B' if target.name not in ('prod', 'qa') else 'A' }}",
        "{{ 'B' if target.name == 'dev' else 'A' if target.name in ('prod', 'qa') }}",
    )
    for value in spellings:
        assert render_config({"a": value}, target_name="prod", file="f") == ({"a": "A"}, ())


def test_a_conditional_outside_the_grammar_is_reported_whatever_the_target() -> None:
    for value in (
        "{{ 'A' if target.name in ('prod') + ['qa'] else 'B' }}",
        "{{ 'A' if target.name in (prod, qa) else 'B' }}",
        "{{ 'A' if target.name == 'prod' else target.database }}",
        "{{ 'A' if target.name }}",
    ):
        for target in ("prod", "dev"):
            resolved, [diagnostic] = render_config({"a": value}, target_name=target, file="f")
            assert (resolved, diagnostic.code) == ({}, "SST-CFG058")


def test_a_conditional_with_no_arm_for_the_target_is_reported_and_removed() -> None:
    value = "{{ 'PROD_DB' if target.name == 'prod' }}"
    tree = {"catalog": {"+database": value, "+schema": "S"}, "top": value}
    resolved, (diagnostic, top) = render_config(tree, target_name="dev", file="config/sst.yml")
    # Removed, so its reader sees the key unset rather than parsing the template as a name.
    assert resolved == {"catalog": {"+schema": "S"}}
    assert (diagnostic.code, diagnostic.subject) == ("SST-CFG016", "config:catalog.+database")
    assert diagnostic.message == "+database does not resolve for target 'dev'"
    assert diagnostic.origin is not None and diagnostic.origin.file == "config/sst.yml"
    assert top.subject == "config:top"


def test_var_resolves_inside_a_value_and_lists_are_rendered_too() -> None:
    tree = {"vars": {"schema": "SALES"}, "list": ["{{ var('schema') }}.X", 3], "flag": True}
    resolved, diagnostics = render_config(tree, target_name=None, file="sst_config.yml")
    assert (resolved["list"], resolved["flag"], diagnostics) == (["SALES.X", 3], True, ())
    _, [missing] = render_config({"vars": "not a mapping", "a": "{{ var('x') }}"}, target_name=None, file="f")
    assert (missing.code, missing.subject) == ("SST-CFG029", "config:a")


def test_a_list_item_whose_var_fails_is_dropped_and_the_rest_kept() -> None:
    tree = {"vars": {"a": "A"}, "list": ["{{ var('a') }}", "{{ var('nope') }}", "x"]}
    resolved, [missing] = render_config(tree, target_name=None, file="f")
    assert resolved["list"] == ["A", "x"]
    assert (missing.code, missing.subject) == ("SST-CFG029", "config:list.1")


def test_a_null_variable_is_empty_not_the_text_none() -> None:
    tree = {"vars": {"sch": None}, "agents": {"+schema": "{{ var('sch') }}", "+database": "DB"}}
    resolved, [empty] = render_config(tree, target_name=None, file="sst_config.yml")
    assert (empty.code, empty.subject) == ("SST-REF009", "config:agents.+schema")
    assert resolved["agents"] == {"+database": "DB"}


def test_a_value_that_is_wholly_a_var_holding_a_conditional_resolves_as_that_conditional() -> None:
    tree = {
        "vars": {
            "production_targets": ["prod", "ci_prod", "prefect_prod"],
            "sst_db": "{{ 'SST' if target.name in var('production_targets') else '' }}",
        },
        "state": {"+database": "{{ var('sst_db') }}"},
        "skills": {"+database": "{{ var('sst_db') }}"},
    }
    for target, expected in (("prefect_prod", "SST"), ("dev", "")):
        resolved, diagnostics = render_config(tree, target_name=target, file="f")
        assert (resolved["state"], resolved["skills"], diagnostics) == (
            {"+database": expected},
            {"+database": expected},
            (),
        )
        # vars: is never resolved in place.
        assert resolved["vars"] == tree["vars"]


def test_a_malformed_conditional_in_a_var_is_reported_at_each_key_that_uses_it() -> None:
    tree = {"vars": {"db": "{{ 'A' if target.name.startswith('p') }}"}, "a": "{{ var('db') }}", "b": "{{ var('db') }}"}
    resolved, diagnostics = render_config(tree, target_name="prod", file="f")
    assert [(d.code, d.subject) for d in diagnostics] == [("SST-CFG058", "config:a"), ("SST-CFG058", "config:b")]
    assert resolved == {"vars": tree["vars"]}


def test_a_conditional_held_only_in_vars_still_needs_the_target_name() -> None:
    held = {"vars": {"db": "{{ 'A' if target.name == 'prod' else 'B' }}"}, "a": "{{ var('db') }}"}
    assert has_target_conditional(held)
    assert not has_target_conditional({"vars": {"db": "PLAIN", "n": 3}, "a": "{{ var('db') }}"})


def test_a_membership_var_that_is_undeclared_or_empty_reports_as_any_var_does() -> None:
    for tree_vars, code in (({}, "SST-CFG029"), ({"t": None}, "SST-REF009")):
        tree = {"vars": tree_vars, "a": "{{ 'A' if target.name in var('t') else 'B' }}"}
        resolved, [diagnostic] = render_config(tree, target_name="prod", file="f")
        assert (resolved, diagnostic.code, diagnostic.subject) == ({"vars": tree_vars}, code, "config:a")
