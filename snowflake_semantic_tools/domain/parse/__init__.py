"""Parse: scan authored text for the structure SST reads out of it, keeping exact source positions.

`template` scans the ``{{ function('arg') }}`` reference dialect in YAML scalars, and
`skill_references` finds the file paths a skill's Markdown names. Scanners return what they
found and never resolve it. Importers name the submodule; nothing is re-exported here.
"""
