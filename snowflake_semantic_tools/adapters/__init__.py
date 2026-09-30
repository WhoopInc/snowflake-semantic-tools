"""`adapters/` -- the outward edge. Third-party SDKs live here and nowhere else.

MAY IMPORT: `domain/ports/`, `domain/model/`, third-party SDKs.

MAY NOT IMPORT: `app/`, `cli/`, or `domain/render/`.

The four subpackages -- `yaml`, `dbt`, `snowflake`, `fs` -- never import one another, so
each stays replaceable and testable alone. A top-level module here may compose them.
`pyproject.toml` enforces both rules as import-linter contracts.
"""
