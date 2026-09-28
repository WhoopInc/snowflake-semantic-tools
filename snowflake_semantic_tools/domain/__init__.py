"""`domain/` -- the pure ring. No I/O, no clock, no environment.

MAY IMPORT: stdlib `typing`, `dataclasses`, `enum`, `re` only.

MAY NOT IMPORT: anything else -- including `yaml`, `pathlib` I/O, `datetime.now`,
`os.environ`, `snowflake.connector`, `click`, and every other ring.

WHY THE RULE IS THIS TIGHT. Anything needing the wall clock, the working directory,
the environment or a file read takes it as a PARAMETER. That is what makes rendering
a pure function of its input, which is what makes the manifest byte-reproducible,
which is what makes golden-file testing possible at all. A single `datetime.now()` in
here would make every golden non-deterministic.

The rule is enforced by the `import-linter` contract in `pyproject.toml`, not by
convention -- an unenforced rule is a comment.
"""
