"""Pure resolution: turn authored references into the values the renderers read.

`template` replaces the template calls in a scalar, `members` attaches semantic members to
their views, and `eval_name` resolves the closed eval-name template grammar. This module
re-exports `members`' public names.
"""

from snowflake_semantic_tools.domain.resolve.members import attach_members, attach_view_members, effective_tables

__all__ = ["attach_members", "attach_view_members", "effective_tables"]
