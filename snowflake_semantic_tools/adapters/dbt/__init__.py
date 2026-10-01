"""dbt artifact adapters."""

from snowflake_semantic_tools.adapters.dbt.manifest import load_manifest_catalog

__all__ = ["load_manifest_catalog"]
