"""Validation codes 870-889 (VAL): Snowflake CoWork profiles.

CoWork profiles are a Snowflake private preview (PrPr) feature; these codes follow the preview's
contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

A profile's subagent list and the tools it carries beside its router.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = ()
