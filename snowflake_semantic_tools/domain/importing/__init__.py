"""Pure mapping of live objects to the project files that would publish them, for `sst import`.

`model` holds what an import produces; `references` the project's names it maps live names to;
`placement` where each object's files go; and one module per type maps a live object: `agent`
(agents and CoWork profiles), `semantic_view`, with `expressions` rewriting its SQL, `tool`,
`skill` (skills and plugins), and `evals`. Nothing here reads Snowflake or the filesystem.
"""
