"""Snowflake Semantic Tools: a dbt-native compiler and publisher for Snowflake semantic views,
Cortex Agents, agent tools, evaluations, skills, plugins, and CoCo Desktop profiles.

The command line (`sst`) is the interface; the package exposes no Python API beyond its version.
"""

from snowflake_semantic_tools._version import __version__

__all__ = ["__version__"]
