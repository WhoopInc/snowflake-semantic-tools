"""SST-REG024: a ref-field policy is bound but undeclared, or declared and neither bound nor explained.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.resolve.ref_fields import (
    REF_FIELDS,
    REF_POLICIES,
    UNBOUND_POLICIES,
    check_ref_policies,
    ref_field,
)
from snowflake_semantic_tools.domain.resolve.template import METRIC_EXPR
from tests.helpers.registry_types import refused


def test_sst_reg024_fires() -> None:
    refused(
        lambda: check_ref_policies({"metric_expr": METRIC_EXPR}, {}, {}),
        "SST-REG024",
        "ref policy 'metric_expr': it is bound to no field and states no reason",
    )


def test_sst_reg024_fires_for_an_undeclared_policy() -> None:
    refused(
        lambda: check_ref_policies({}, {"metric.expression": "metric_expr"}, {}),
        "SST-REG024",
        "ref policy 'metric_expr': field 'metric.expression' is bound to it, but it is not declared",
    )


def test_sst_reg024_silent() -> None:
    check_ref_policies(REF_POLICIES, REF_FIELDS, UNBOUND_POLICIES)
    assert ref_field("metric.expression") is METRIC_EXPR
