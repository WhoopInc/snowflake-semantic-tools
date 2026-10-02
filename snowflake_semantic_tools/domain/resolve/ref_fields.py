"""Which template-call policy each authored field is resolved under, checked once at import.

A field reaches `resolve_scalar` through `ref_field`, so the policy it resolves under is the one
`REF_FIELDS` binds to its name. Every policy `REF_POLICIES` declares is either bound to some
field or listed in `UNBOUND_POLICIES` with the reason nothing binds it, and every binding names
a declared policy; `check_ref_policies` refuses a table that breaks either rule.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics.integrity import registry_fault
from snowflake_semantic_tools.domain.resolve.template import (
    CUSTOM_INSTRUCTION_ITEM,
    DESCRIPTION,
    FILTER_EXPR,
    METRIC_EXPR,
    TAG_NAME,
    VQR_SQL,
    RefPolicy,
)

REF_POLICIES: Mapping[str, RefPolicy] = MappingProxyType(
    {
        "metric_expr": METRIC_EXPR,
        "filter_expr": FILTER_EXPR,
        "vqr_sql": VQR_SQL,
        "custom_instruction_item": CUSTOM_INSTRUCTION_ITEM,
        "tag_name": TAG_NAME,
        "description": DESCRIPTION,
    }
)

# Each field resolved through `resolve_scalar`, by the name its diagnostics give it.
REF_FIELDS: Mapping[str, str] = MappingProxyType(
    {
        "metric.expression": "metric_expr",
        "metric.window": "metric_expr",
        "filter.expression": "filter_expr",
        "verified_query.sql": "vqr_sql",
    }
)

UNBOUND_POLICIES: Mapping[str, str] = MappingProxyType(
    {
        "custom_instruction_item": (
            "a view's custom_instructions entries are resolved by the semantic loader's own pass, "
            "which refuses the same calls"
        ),
        "tag_name": "no authored field names a tag through a template call yet",
        "description": "descriptions are rendered as written and never resolved",
    }
)


def ref_field(field: str) -> RefPolicy:
    """Return the policy `REF_FIELDS` binds to `field`.

    Raises:
        KeyError: `field` is not bound.
    """
    return REF_POLICIES[REF_FIELDS[field]]


def check_ref_policies(
    policies: Mapping[str, RefPolicy], fields: Mapping[str, str], unbound: Mapping[str, str]
) -> None:
    """Refuse a binding to an undeclared policy, and a declared policy neither bound nor explained.

    Bindings are checked in field order, then the policies in declaration order.

    Raises:
        RegistryIntegrityError: SST-REG024.
    """
    for field, policy in fields.items():
        if policy not in policies:
            raise registry_fault(
                "SST-REG024", policy=policy, detail=f"field '{field}' is bound to it, but it is not declared"
            )
    bound = set(fields.values())
    for policy in policies:
        reason = unbound.get(policy, "").strip()
        if policy in bound and reason:
            raise registry_fault(
                "SST-REG024", policy=policy, detail="it is bound to a field and also explained as unbound"
            )
        if policy not in bound and not reason:
            raise registry_fault("SST-REG024", policy=policy, detail="it is bound to no field and states no reason")


check_ref_policies(REF_POLICIES, REF_FIELDS, UNBOUND_POLICIES)
