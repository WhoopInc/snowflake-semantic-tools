"""Shared test support: in-memory ports, the values tests build on, the gates, and the `run_recorded_*` scripts.

Test modules import these as `tests.helpers.<module>` (pytest puts the repository root on
`sys.path`, see `pythonpath` in pyproject.toml). This is the only place tests share code:
a test never imports a conftest or another test module (`tests/unit/test_import_style.py`).
The `run_recorded_*` scripts run as plain files, so they import their siblings by bare
module name.
"""
