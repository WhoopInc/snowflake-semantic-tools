"""SST-VAL339: an enrich run sampled values shaped like personal data, and wrote none of them."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.enrich import withhold_pii_samples
from snowflake_semantic_tools.domain.model.dbt import DbtModel

ORDERS = DbtModel("model.p.orders", "orders", "DB.S.ORDERS", (), (), ())


def test_sst_val339_fires() -> None:
    kept, [found] = withhold_pii_samples(ORDERS, {"contact": ["jane.doe@example.com"], "status": ["open"]})
    assert dict(kept) == {"status": ["open"]}
    assert found.code == "SST-VAL339"
    assert found.severity is Severity.WARNING
    assert found.message == (
        "model 'orders': column 'contact' was sampled and holds an email address, so no sample values were written"
    )
    assert found.subject == "dbt_column:orders.contact"
    assert "jane" not in found.message


def test_sst_val339_silent() -> None:
    kept, found = withhold_pii_samples(ORDERS, {"firmware_version": ["4.10.2.1"], "status": ["open"]})
    assert found == () and set(kept) == {"firmware_version", "status"}
