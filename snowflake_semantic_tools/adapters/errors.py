"""Failures raised while an adapter turns project files into domain values."""

from __future__ import annotations

from ..domain.model.diagnostic import Diagnostic


class ProjectError(Exception):
    """A project input is unreadable or violates the compiler boundary."""

    def __init__(self, message: str, *, diagnostics: tuple[Diagnostic, ...] = ()) -> None:
        super().__init__(message)
        self.diagnostics = diagnostics
