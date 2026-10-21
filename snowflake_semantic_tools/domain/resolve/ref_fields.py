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
    ROUTINE_BODY,
    STAGE_PATH,
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
        "stage_path": STAGE_PATH,
        "routine_body": ROUTINE_BODY,
    }
)

# Each field resolved through `resolve_scalar`, by the name its diagnostics give it.
REF_FIELDS: Mapping[str, str] = MappingProxyType(
    {
        "metric.expression": "metric_expr",
        "metric.window": "metric_expr",
        "filter.expression": "filter_expr",
        "tool.where": "filter_expr",
        "verified_query.sql": "vqr_sql",
        "tags.name": "tag_name",
    }
)

UNBOUND_POLICIES: Mapping[str, str] = MappingProxyType(
    {
        "custom_instruction_item": (
            "a view's custom_instructions entries are resolved by the semantic loader's own pass, "
            "which refuses the same calls"
        ),
        "description": "descriptions are rendered as written and never resolved",
        "stage_path": (
            "a stage path takes one opening tool() call, which resolve_stage_path resolves against the "
            "tool catalog rather than through resolve_scalar"
        ),
        "routine_body": (
            "a routine's body is code in another language, which expand_body scans for these calls "
            "alone, leaving every other `{{` as written"
        ),
    }
)


def ref_field(field: str) -> RefPolicy:
    """Return the policy `REF_FIELDS` binds to `field`.

    Raises:
        KeyError: `field` is not bound.
    """
    return REF_POLICIES[REF_FIELDS[field]]


# Every authored field that names a file or a member, by where it is written, with the template
# functions it takes, the first being the one a bare value is told to use. Most are read through
# `reference_functions` by `calls.required_call`; a stage path opens with its call
# (`resolve_stage_path`), and a view's tables, a relationship's endpoints and a view's custom
# instructions have readers of their own that refuse the same values. docs/guides/references.md
# lists each, with the fields left literal, and a test keeps the two in step.
REFERENCE_FIELDS: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "semantic_view.tables": ("ref", "source"),
        "semantic_view.columns": ("ref",),
        "semantic_view.metrics": ("metric",),
        "semantic_view.relationships": ("relationship",),
        "semantic_view.custom_instructions": ("custom_instructions",),
        "semantic_view.config.tags.name": ("tag",),
        "relationship.left_table": ("ref",),
        "relationship.right_table": ("ref",),
        "metric.using_relationships": ("relationship",),
        "metric.non_additive_dimensions.ref": ("ref",),
        "verified_query.sql": ("file",),
        "agent.spec.instructions": ("file",),
        "agent.spec.tools.semantic_view": ("semantic_view",),
        "agent.spec.tools.search_service": ("tool",),
        "agent.spec.tools.identifier": ("tool",),
        "agent.spec.tools.agent": ("agent",),
        "agent.spec.tools.stage_path": ("tool",),
        "agent.spec.skills.source.path": ("skill", "plugin", "extension"),
        "agent.spec.skills.name": ("skill",),
        "agent.spec.skills.source.version": ("var",),
        "agent.config.tags.name": ("tag",),
        "agent.evals.dataset": ("eval_dataset",),
        "agent.evals.config": ("eval_config",),
        # A CoWork profile's fields, a Snowflake private preview: its subagents name an agent this
        # project publishes, or an `agent` tool member for one it does not.
        "cowork_profile.spec.instructions": ("file",),
        "cowork_profile.spec.subagents.agent": ("agent", "tool"),
        "cowork_profile.spec.skills.source.path": ("skill", "plugin", "extension"),
        "cowork_profile.spec.skills.name": ("skill",),
        "cowork_profile.spec.skills.source.version": ("var",),
        "cowork_profile.config.tags.name": ("tag",),
        "cowork_profile.evals.dataset": ("eval_dataset",),
        "cowork_profile.evals.config": ("eval_config",),
        "eval_config.metrics.custom": ("eval_metric",),
        # The subagents a CoWork profile eval's row expects, named as the profile names them.
        "eval_dataset.ground_truth_subagents": ("agent", "tool"),
        "tool.on": ("ref",),
        "tool.columns": ("ref",),
        "tool.body": ("file",),
        "tool.imports": ("tool",),
        "plugin.skills": ("skill",),
        "profile.skills": ("skill",),
        "profile.plugins": ("plugin",),
        "profile.commands": ("command",),
        "profile.hooks": ("hook",),
        "profile.mcp_servers": ("mcp_server",),
        "hook.script": ("file",),
        "command.skill": ("skill",),
    }
)


def reference_functions(field: str) -> tuple[str, ...]:
    """Return the template functions the reference field `field` takes, the one to suggest first.

    Raises:
        KeyError: `field` is not a reference field.
    """
    return REFERENCE_FIELDS[field]


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
