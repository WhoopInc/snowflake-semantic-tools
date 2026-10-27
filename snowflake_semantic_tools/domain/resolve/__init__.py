"""Pure resolution: turn authored references into the values the renderers read.

`calls` holds the rules every template call obeys and `template` replaces the calls in a
scalar; `depth` bounds how deep `metric()` references nest. `membership` is the member
resolution step -- it attaches members to their views through `members`, and reports what
that means through `membership_tables` and `membership_reach` -- and `rendered` holds the built
views to what attachment placed. `eval_name` resolves the closed eval-name template grammar.
This module re-exports `members`' public names.
"""

from snowflake_semantic_tools.domain.resolve.members import attach_members, attach_view_members, effective_tables

__all__ = ["attach_members", "attach_view_members", "effective_tables"]
