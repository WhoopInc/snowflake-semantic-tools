"""Install the test network guard in a subprocess an offline test starts.

Python imports `sitecustomize` at startup from the first `sys.path` entry that holds one, and
`network_guard.guarded_environment` puts this directory first on the child's `PYTHONPATH`. The
guard is installed only when the environment asks for it, so a child that inherits the path
without the variable runs unguarded.
"""

from __future__ import annotations

import os

if os.environ.get("SST_TEST_NETWORK_GUARD") == "1":
    from tests.helpers import network_guard

    network_guard.install_in_subprocess()
