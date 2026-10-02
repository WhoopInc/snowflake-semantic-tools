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


def test_a_conditional_with_no_arm_for_the_target_is_reported_and_kept() -> None:
    value = "{{ 'PROD_DB' if target.name == 'prod' }}"
    resolved, [diagnostic] = render_config({"catalog": {"+database": value}}, target_name="dev", file="config/sst.yml")
    assert resolved["catalog"]["+database"] == value
    assert (diagnostic.code, diagnostic.subject) == ("SST-CFG016", "config:catalog.+database")
    assert diagnostic.message == "+database does not resolve for target 'dev'"
    assert diagnostic.origin is not None and diagnostic.origin.file == "config/sst.yml"


def test_var_resolves_inside_a_value_and_lists_are_rendered_too() -> None:
    tree = {"vars": {"schema": "SALES"}, "list": ["{{ var('schema') }}.X", 3], "flag": True}
    resolved, diagnostics = render_config(tree, target_name=None, file="sst_config.yml")
    assert (resolved["list"], resolved["flag"], diagnostics) == (["SALES.X", 3], True, ())
    _, [missing] = render_config({"vars": "not a mapping", "a": "{{ var('x') }}"}, target_name=None, file="f")
    assert (missing.code, missing.subject) == ("SST-CFG029", "config:a")
