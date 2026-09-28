"""`adapters/` -- the outward edge. Third-party SDKs live here and nowhere else.

MAY IMPORT: `domain/ports/`, `domain/model/`, third-party SDKs.

MAY NOT IMPORT: `app/`, `cli/`, or another adapter.

The no-sibling-imports rule is what keeps an adapter replaceable: if the YAML loader
imported the Snowflake adapter, neither could be swapped or tested alone.
"""
