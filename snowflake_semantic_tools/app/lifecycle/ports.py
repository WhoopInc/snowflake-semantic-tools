"""The Snowflake port roles a composite publication runs through, apart from the handlers.

Apply types its port with `CatalogPublicationPort`, and the composite handlers classify
failures with apply's error classifier. Keeping the roles here, with nothing above `domain`
imported, lets either side load first.
"""

from __future__ import annotations

from typing import Protocol

from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagePort


class PublicationPort(ExecutionPort, StagePort, Protocol):
    """The Snowflake roles every publication run uses: running its statements and moving its files."""


class CatalogPublicationPort(CatalogPort, PublicationPort, Protocol):
    """A publication port that also reads the catalog, as the eval and extension handlers do."""
