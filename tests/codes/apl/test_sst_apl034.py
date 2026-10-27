"""SST-APL034: `sst state convert` refused, with nothing changed."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.state_convert import STATE_TABLE, Catalog, Connector


def test_sst_apl034_fires() -> None:
    with pytest.raises(SnowflakePortError) as raised:
        Connector(Catalog(locks=(("prod", "run-a"),))).convert_state_tables(STATE_TABLE, "hybrid")
    assert raised.value.diagnostic is not None and raised.value.diagnostic.code == "SST-APL034"


def test_sst_apl034_silent() -> None:
    assert Connector(Catalog()).convert_state_tables(STATE_TABLE, "hybrid").to_kind == "hybrid"
