"""A composite plan's reads in a schema apply will create find nothing there, without reading it.

Snowflake refuses each existence read in a schema that does not exist (002043), as the fake does
for one `preflight.missing_schemas` lists; the reads a composite plan makes through
`_AbsentSchemaReads` answer as for an object not there instead, and every other read, and every
read in a schema that exists, goes to the session as it is.
"""

from __future__ import annotations

from typing import cast

import pytest

from snowflake_semantic_tools.app.plan_artifacts import _AbsentSchemaReads
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.snowflake_fake import FakeSnowflake

ABSENT = QualifiedName.parse("DB.NEW.THING")
PRESENT = QualifiedName.parse("DB.LIVE.THING")


def _session() -> FakeSnowflake:
    session = FakeSnowflake(existing=(PRESENT.sql,))
    session.preflight.missing_schemas.add("DB.NEW")
    session.stage_types[PRESENT.sql] = "INTERNAL"
    session.tables[PRESENT.sql] = (("ID", "NUMBER"),)
    return session


def test_each_existence_read_in_an_absent_schema_finds_nothing_without_reading_it() -> None:
    session = _session()
    with pytest.raises(SnowflakePortError, match="002043"):
        session.stage_type(ABSENT)
    reads = _AbsentSchemaReads(session, frozenset({("DB", "NEW")}))
    assert reads.stage_type(ABSENT) is None
    assert reads.observe_extension(ABSENT) is None
    assert reads.table_columns(ABSENT) is None
    assert reads.object_exists("STAGE", ABSENT) is False


def test_reads_in_a_schema_that_exists_and_every_other_read_go_to_the_session() -> None:
    session = _session()
    reads = _AbsentSchemaReads(session, frozenset({("DB", "NEW")}))
    assert reads.stage_type(PRESENT) == "INTERNAL"
    assert reads.observe_extension(PRESENT) is None
    assert reads.table_columns(PRESENT) == (("ID", "NUMBER"),)
    assert reads.object_exists("TABLE", PRESENT) is True
    # Any other read is the session's own, delegated as it is.
    assert cast(FakeSnowflake, reads).list_location("@DB.LIVE.STAGE/prefix") == session.list_location(
        "@DB.LIVE.STAGE/prefix"
    )
