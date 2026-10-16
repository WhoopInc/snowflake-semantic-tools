"""SST-PLN103: a DESCRIBE or GET_DDL a drift digest needs failed, so plan did not compare what it reads."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.drift import observed_digest
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import DRIFT_CHECK_BASIC, PublishShape, RenderedArtifact
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import statement

PROCEDURE = QualifiedName.parse("DB.S.ORDER_STATUS")


def _procedure() -> RenderedArtifact:
    artifact = RenderedArtifact.create(
        key="tool:order_status",
        artifact_type="tool",
        target=PROCEDURE,
        ddl=statement("create or replace procedure DB.S.ORDER_STATUS(ID VARCHAR)"),
        shape=PublishShape("PROCEDURE", routine_signature=("VARCHAR",)),
    )
    return replace(artifact, routine_signature=("VARCHAR",))


def test_sst_pln103_fires() -> None:
    port = FakeSnowflake()
    port.fail("describe_routine", SnowflakePortError("insufficient privileges"))
    digest, [diagnostic] = observed_digest(
        port, "PROCEDURE", PROCEDURE, _procedure(), shown={"is_secure": "N"}, newest_spec=None
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "DB.S.ORDER_STATUS: DESCRIBE PROCEDURE failed, so plan did not compare what it reads: insufficient privileges"
    )
    # What SHOW lists is still digested.
    assert digest is not None and digest.startswith("secure=")


def test_sst_pln103_silent() -> None:
    port = FakeSnowflake()
    port.routine_descriptions["PROCEDURE DB.S.ORDER_STATUS(VARCHAR)"] = {"execute as": "CALLER", "body": "x"}
    _, quiet = observed_digest(port, "PROCEDURE", PROCEDURE, _procedure(), shown={"is_secure": "N"}, newest_spec=None)
    assert quiet == ()
    # A basic check never reads DESCRIBE, so a refused one is never reported.
    port.fail("describe_routine", SnowflakePortError("insufficient privileges"))
    _, basic = observed_digest(
        port, "PROCEDURE", PROCEDURE, _procedure(), shown={}, newest_spec=None, drift_check=DRIFT_CHECK_BASIC
    )
    assert basic == ()
