"""Every registered code, one module per code family, and each subsystem's reference title.

Each module exports `SPECS`, its codes, and `TITLE`, the title in `SUBSYSTEMS` of the
prefix it is named for (LOD for `loading`, SNO for `snowflake`, INT for `internal`, and
VAL for each `val_*` module). The package `__init__` builds `ERROR_REGISTRY` from every
module's `SPECS`. A new code goes in the module for its prefix:

- `cfg`: CFG
- `prs`: PRS
- `loading`: LOD, DBT, and MEM
- `ref`: REF
- `val_semantic_view`, `val_agent`, `val_tool`, `val_eval`, `val_extension`: VAL by
  hundreds -- 0xx to 4xx, 5xx, 6xx, 7xx, and 8xx
- `man`: MAN
- `pln`: PLN
- `apl`: APL
- `snowflake`: SNO and PRT
- `internal`: INT and RND

`SUBSYSTEMS` maps each prefix to its section title in the generated error reference, in
section order. It is keyed by prefix, not by module, so which module holds a code never
changes the reference; a new prefix needs an entry here, or rendering the reference fails.
"""

from __future__ import annotations

from collections.abc import Mapping

SUBSYSTEMS: Mapping[str, str] = {
    "CFG": "Configuration",
    "PRS": "Parsing",
    "LOD": "Loading",
    "REF": "References",
    "MEM": "Membership",
    "VAL": "Validation",
    "DBT": "dbt",
    "RND": "Rendering",
    "MAN": "Manifest and state",
    "PLN": "Planning",
    "APL": "Apply",
    "SNO": "Snowflake",
    "PRT": "External systems",
    "INT": "Internal",
}
