"""Failures raised while an adapter turns project files into domain values."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic


class ProjectError(Exception):
    """A project input is unreadable or violates the compiler boundary.

    Every refusal carries the coded diagnostics that report it, so a command that stops on one
    still names an SST code. The message defaults to the first diagnostic's.

    Attributes:
        diagnostics: What reports the refusal; never empty. One raised with none carries
            SST-INT010 for its message instead, since an uncoded refusal is a defect in SST.
    """

    def __init__(self, message: str | None = None, *, diagnostics: tuple[Diagnostic, ...]) -> None:
        if not diagnostics:
            diagnostics = (D("SST-INT010", detail=message or "a project input cannot be used"),)
        super().__init__(diagnostics[0].message if message is None else message)
        self.diagnostics = diagnostics
