"""Suite-wide pytest configuration: the Hypothesis profiles property tests run under.

`ci` is deterministic, so a property failure on a pull request reproduces on rerun, and has
no deadline, because shared runners are too noisy for per-example timing to mean anything.
`dev`, the default, keeps Hypothesis's random search and its local example database. Pick
one with HYPOTHESIS_PROFILE or pytest's `--hypothesis-profile`.
"""

from __future__ import annotations

import os

from hypothesis import settings

settings.register_profile("ci", deadline=None, derandomize=True, database=None, print_blob=True)
settings.register_profile("dev", deadline=None)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))
