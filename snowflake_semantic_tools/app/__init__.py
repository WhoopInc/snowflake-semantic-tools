"""`app/` -- use cases. Orchestrates `domain/` through injected ports.

MAY IMPORT: `domain/` only.

MAY NOT IMPORT: `adapters/`, `cli/`, `click`, `snowflake.connector`, `yaml`.

WHY. A use case receives its ports as constructor arguments, so a test passes in
in-memory implementations that are real code rather than `MagicMock`. 0.3 has no
`conftest.py` and roughly 570 inline mock usages; the cause is that there is nowhere
to inject. This ring is that place.
"""
