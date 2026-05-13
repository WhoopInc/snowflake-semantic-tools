"""Metadata store abstraction for SST semantic metadata access."""

from snowflake_semantic_tools.core.metadata.in_memory_store import InMemoryStore
from snowflake_semantic_tools.core.metadata.store import MetadataStore

__all__ = ["MetadataStore", "InMemoryStore"]
