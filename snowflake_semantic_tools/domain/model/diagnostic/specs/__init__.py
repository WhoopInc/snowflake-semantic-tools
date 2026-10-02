"""Every registered code, one module per code area, and each subsystem's reference title.

Each module exports `SPECS`, its codes sorted by code, and `TITLE`, the title in
`SUBSYSTEMS` of the prefix it is named for. A module is the prefix in lowercase (`int_`
for INT, so it does not shadow the builtin), except VAL, which is split by hundreds:

- `val_shared`: 0xx, names, descriptions, and connected validation
- `val_metric`: 1xx, metrics
- `val_relationship`: 2xx, relationships
- `val_semantic_view`: 3xx, column metadata and keys
- `val_filter`: 4xx, filters, custom instructions, and verified queries
- `val_agent`: 5xx, agents
- `val_tool`: 6xx, tools
- `val_eval`: 7xx, evals
- `val_skill`: 8xx, skills, plugins, and profiles

A new code goes in the module for its prefix, or its band for VAL. A module whose area
has no code yet (`reg`, `dis`) exports an empty `SPECS`, so the next code has a home.
The package `__init__` builds `ERROR_REGISTRY` from every module's `SPECS`.

`SUBSYSTEMS` maps each prefix to its section title in the generated error reference, in
section order. It is keyed by prefix, not by module, so which module holds a code never
changes the reference; a prefix with no codes gets no section, and a new prefix needs an
entry here, or rendering the reference fails.
"""

from __future__ import annotations

from collections.abc import Mapping

SUBSYSTEMS: Mapping[str, str] = {
    "REG": "Registry",
    "CFG": "Configuration",
    "DIS": "Discovery",
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
