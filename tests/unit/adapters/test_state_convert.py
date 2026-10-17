"""`convert_state_tables`: copy the state table into the other kind, swap names, keep backups."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.state_convert import STATE_TABLE, Catalog, Connector


def test_a_standard_state_table_becomes_hybrid_and_the_old_ones_are_kept_as_backups() -> None:
    catalog = Catalog(rows=3)
    conversion = Connector(catalog).convert_state_tables(STATE_TABLE, "hybrid")
    assert (conversion.from_kind, conversion.to_kind, conversion.rows) == ("standard", "hybrid", 3)
    assert conversion.state_backup.sql == "DB.S.SST_STATE_STANDARD_BAK"
    assert conversion.lock_backup is not None and conversion.lock_backup.sql == "DB.S.SST_STATE_LOCK_STANDARD_BAK"
    assert catalog.tables == {
        "SST_STATE": ("YES", 3),
        "SST_STATE_LOCK": ("YES", 0),
        "SST_STATE_STANDARD_BAK": ("NO", 3),
        "SST_STATE_LOCK_STANDARD_BAK": ("NO", 1),
    }
    insert = next(statement for statement in catalog.statements if statement.startswith("INSERT INTO"))
    # Only the columns the old table holds are copied.
    assert "(TARGET_NAME, ARTIFACT_KEY, FINGERPRINT, QUALIFIED_NAME, MANIFEST_ID, STATE_MANIFEST_ID)" in insert
    assert "LIVE_DIGEST" not in insert


def test_a_hybrid_state_table_becomes_standard() -> None:
    catalog = Catalog(kind="YES")
    conversion = Connector(catalog).convert_state_tables(STATE_TABLE, "standard")
    assert conversion.state_backup.sql == "DB.S.SST_STATE_HYBRID_BAK"
    assert catalog.tables["SST_STATE"] == ("NO", 3) and catalog.tables["SST_STATE_LOCK"] == ("NO", 0)


@pytest.mark.parametrize(
    ("catalog", "to_kind", "named"),
    [
        (Catalog(locks=(("prod", "run-a"),)), "hybrid", "a run holds the lock on prod (run run-a)"),
        (Catalog(kind="YES"), "hybrid", "it is already a hybrid table"),
        (Catalog(), "standard", "it is already a standard table"),
    ],
    ids=["live-lock", "already-hybrid", "already-standard"],
)
def test_a_conversion_that_cannot_run_is_refused_with_nothing_changed(
    catalog: Catalog, to_kind: str, named: str
) -> None:
    before = dict(catalog.tables)
    with pytest.raises(SnowflakePortError) as raised:
        Connector(catalog).convert_state_tables(STATE_TABLE, to_kind)
    diagnostic = raised.value.diagnostic
    assert diagnostic is not None and diagnostic.code == "SST-APL034" and named in diagnostic.message
    assert catalog.tables == before


def test_a_taken_backup_name_or_a_missing_state_table_is_refused() -> None:
    taken = Catalog()
    taken.tables["SST_STATE_STANDARD_BAK"] = ("NO", 9)
    with pytest.raises(SnowflakePortError, match="SST_STATE_STANDARD_BAK already exists"):
        Connector(taken).convert_state_tables(STATE_TABLE, "hybrid")
    missing = Catalog()
    missing.tables.clear()
    with pytest.raises(SnowflakePortError, match="does not exist"):
        Connector(missing).convert_state_tables(STATE_TABLE, "hybrid")


def test_rows_written_during_the_copy_drop_the_copy_and_refuse() -> None:
    catalog = Catalog()
    catalog.write_during_copy = 1
    with pytest.raises(SnowflakePortError, match="rows changed while they were copied"):
        Connector(catalog).convert_state_tables(STATE_TABLE, "hybrid")
    assert set(catalog.tables) == {"SST_STATE", "SST_STATE_LOCK"}
    assert catalog.tables["SST_STATE"][0] == "NO"


@pytest.mark.parametrize(
    ("errno", "message", "code"),
    [
        (391412, "Cannot create hybrid table within transient database or schema.", "SST-APL030"),
        # Illustrative wording: the error of an account without hybrid tables is not measured.
        (1234, "Hybrid tables are not available for this account.", "SST-APL035"),
    ],
    ids=["transient-schema", "account-without-hybrid-tables"],
)
@pytest.mark.parametrize("table", ["SST_STATE_SST_CONVERT", "SST_STATE_LOCK"], ids=["copy", "lock"])
def test_a_hybrid_table_snowflake_will_not_create_is_named(errno: int, message: str, code: str, table: str) -> None:
    catalog = Catalog()
    catalog.create_fails = (table, errno, message)
    with pytest.raises(SnowflakePortError) as raised:
        Connector(catalog).convert_state_tables(STATE_TABLE, "hybrid")
    diagnostic = raised.value.diagnostic
    assert diagnostic is not None and diagnostic.code == code
    named = "DB.S.SST_STATE_LOCK" if table == "SST_STATE_LOCK" else "DB.S.SST_STATE"
    assert diagnostic.message.startswith(f"{named}: ")
    # The copy is dropped; a failed lock table leaves the converted state table for the next apply.
    assert "SST_STATE_SST_CONVERT" not in catalog.tables


def test_a_standard_table_snowflake_will_not_create_is_reported_as_it_failed() -> None:
    catalog = Catalog(kind="YES")
    catalog.create_fails = ("SST_STATE_SST_CONVERT", 1234, "Hybrid tables are not available for this account.")
    with pytest.raises(SnowflakePortError) as raised:
        Connector(catalog).convert_state_tables(STATE_TABLE, "standard")
    assert raised.value.diagnostic is None and raised.value.errno == 1234
