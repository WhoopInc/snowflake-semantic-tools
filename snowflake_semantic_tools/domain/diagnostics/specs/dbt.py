"""dbt metadata codes (DBT): the SST metadata on dbt models and columns."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "dbt"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-DBT003",
        Severity.ERROR,
        "Unknown dbt semantic role",
        "model '{model}': meta.sst role '{found}' is not a known role",
        "correct the role name",
    ),
    spec(
        "SST-DBT004",
        Severity.WARNING,
        "dbt and semantic column types disagree",
        "model '{model}': column '{column}' is {found} in dbt and {expected} in the semantic layer",
        "reconcile the two types, or add a dbt contract",
    ),
    spec(
        "SST-DBT005",
        Severity.ERROR,
        "Key metadata is written in the 0.3 form",
        "model '{model}': meta.sst.{field} is written in the 0.3 form",
        "write primary_key as a list of columns and unique_keys as a list of column lists",
    ),
    spec(
        "SST-DBT030",
        Severity.ERROR,
        "Forbidden meta.sst location key",
        "model '{model}': meta.sst.{key} is forbidden -- delete it",
        "delete the key; relation location comes from dbt's resolved manifest",
    ),
    spec(
        "SST-DBT031",
        Severity.WARNING,
        "Model has no relation to enrich",
        "model '{model}' has no relation, so sst enrich has no columns to read",
        "materialize the model as a table or a view; an ephemeral model has nothing to enrich",
    ),
)
