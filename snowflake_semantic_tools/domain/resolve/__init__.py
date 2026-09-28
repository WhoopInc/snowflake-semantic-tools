"""Pure compiler resolution phases."""

from .members import attach_members, attach_view_members, effective_tables

__all__ = ["attach_members", "attach_view_members", "effective_tables"]
