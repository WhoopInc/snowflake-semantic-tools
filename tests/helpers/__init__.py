"""Shared test support: in-memory ports and the scripts that drive `sst` against recorded observations.

Test modules import these as `tests.helpers.<module>` (pytest puts the repository root on
`sys.path`, see `pythonpath` in pyproject.toml). The `run_recorded_*` scripts run as plain
files, so they import their siblings by bare module name.
"""
