"""Retired numbers: codes the 1.0 catalog withdrew, each with the release that withdrew it.

A retired number is burned. It keeps its meaning in user configuration, baselines, and
history, so `diagnostics.integrity` refuses to register it again (SST-REG017). A code is
retired here, never deleted and reused.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

RETIRED_CODES: Mapping[str, str] = MappingProxyType(
    dict.fromkeys(
        (
            "SST-APL101",
            "SST-APL102",
            "SST-CFG021",
            "SST-CFG022",
            "SST-CFG024",
            "SST-CFG026",
            "SST-CFG027",
            "SST-CFG028",
            "SST-CFG030",
            "SST-DBT007",
            "SST-DBT008",
            "SST-MEM102",
            "SST-PRS108",
            "SST-PRT007",
            "SST-REF016",
            "SST-REF017",
            "SST-REF021",
            "SST-REF024",
            "SST-REF025",
            "SST-REF038",
            "SST-REF200",
            "SST-REF201",
            "SST-SNO021",
            "SST-VAL313",
            "SST-VAL501",
            "SST-VAL502",
            "SST-VAL503",
            "SST-VAL504",
            "SST-VAL505",
            "SST-VAL620",
            "SST-VAL736",
            "SST-VAL749",
            "SST-VAL750",
            "SST-VAL751",
            "SST-VAL752",
            "SST-VAL753",
            "SST-VAL754",
            "SST-VAL756",
            "SST-VAL757",
        ),
        "1.0.0",
    )
)
