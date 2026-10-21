"""Attach each CoWork profile a plan publishes to the account's CoWork object.

CoWork profiles are a Snowflake private preview (PrPr) feature; attaching follows the preview's
contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

CoWork shows a profile only once it is attached to the account's one CoWork object, the
SNOWFLAKE INTELLIGENCE object. Plan reads that object and what is attached to it once
(`AttachmentReading`). Attaching is never part of a profile's definition, so it changes neither
the fingerprint nor the drift digest: a profile detached by hand is attached again, not blocked as
changed out of band. Nothing here detaches a profile, and nothing touches another attachment.
"""

from __future__ import annotations

from dataclasses import dataclass

_Name = tuple[str, str, str]


@dataclass(frozen=True, slots=True)
class AttachmentReading:
    """What plan read of the account's CoWork object: its name, and the agents attached to it.

    Attributes:
        cowork_object: The CoWork object's name as SHOW SNOWFLAKE INTELLIGENCES lists it; None
            when the account has none.
        attached: The agents attached to it, each folded as `QualifiedName.folded`; empty when
            the account has no CoWork object.
    """

    cowork_object: str | None
    attached: frozenset[_Name] = frozenset()
