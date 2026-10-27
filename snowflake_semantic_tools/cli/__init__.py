"""`cli/` -- the composition root, and the ONLY ring permitted to construct an adapter.

MAY IMPORT: everything -- `app/`, `domain/`, `adapters/`, `click`.

This is the asymmetry that makes the other three testable. Wiring happens here, once,
so nothing below needs to know which concrete adapter it was handed.
"""
