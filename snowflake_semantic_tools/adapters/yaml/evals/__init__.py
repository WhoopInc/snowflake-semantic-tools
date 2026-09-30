"""Parse Cortex Agent evaluation datasets, configs, and custom judges.

`catalog` loads every agent's eval files into one validated catalog: `dataset` parses a
dataset file, `config` a config file and the `evals:` defaults of `sst_config.yml`, and
`metrics` the custom judge files; `readers` holds the field readers they share. This module
re-exports the public names, so importers never reach into the submodules.
"""

from __future__ import annotations

from .catalog import load_eval_catalog
from .config import parse_eval_defaults

__all__ = ["load_eval_catalog", "parse_eval_defaults"]
