#!/usr/bin/env python3
"""
Configuration Management

Loads and manages SST configuration from multiple sources with clear precedence:
1. Environment variables (.env file) - highest priority
2. Config file (sst_config.yaml) - organizational defaults
3. Code defaults - minimal fallbacks

This enables:
- Organizations to commit shared defaults
- Developers to override locally with .env
- Secrets to stay out of version control

Note: This module automatically loads .env files when Config is instantiated.
However, when running dbt commands directly (e.g., `dbt compile`), dbt does not
automatically load .env files. To make environment variables available to dbt:
- Use `direnv` (recommended) to auto-load .env files in your shell
- Or export variables manually: `export SNOWFLAKE_ACCOUNT=... && dbt compile`
- Or use poetry-dotenv plugin if using Poetry
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


class Config:
    """
    SST Configuration Manager

    Loads configuration from multiple sources with clear precedence.
    Singleton pattern ensures consistent config across application.
    """

    _instance: Optional["Config"] = None
    _config: Dict[str, Any] = {}

    def __new__(cls):
        """Singleton pattern - only one Config instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        """Load configuration from file and environment variables."""
        # Load .env file first (if available)
        self._load_dotenv()

        # Start with defaults
        self._config = self._get_defaults()

        # Load from config file if it exists
        config_file = self._find_config_file()
        if config_file and config_file.exists():
            file_config = self._load_yaml_config(config_file)
            self._merge_config(self._config, file_config)

        # Override with environment variables (highest priority)
        self._load_env_overrides()

    def _load_dotenv(self):
        """Load .env file from current directory if it exists."""
        try:
            from dotenv import load_dotenv

            env_path = Path.cwd() / ".env"
            if env_path.exists():
                load_dotenv(env_path, override=True)
        except ImportError:
            # python-dotenv not available - environment vars must be set manually
            pass
        except Exception:
            # Silent fail - .env loading is optional
            pass

    def _get_defaults(self) -> Dict[str, Any]:
        """Get minimal code defaults as fallback."""
        return {
            "repository": {
                "url": None,  # No default - must be configured
                "local_path": "./dbt-repo",
                "default_branch": "main",
            },
            "project": {
                "semantic_models_dir": None,  # Required - must be in sst_config.yml
                "dbt_models_dir": None,  # Required - must be in sst_config.yml
            },
            "validation": {"exclude_dirs": [], "strict": False, "verbose": False},
            "enrichment": {
                "distinct_limit": 25,  # Number of distinct values to fetch (accounts for null)
                "sample_values_display_limit": 10,  # Number of sample values to display in YAML
                "generate_synonyms": False,  # Auto-generate synonyms using Cortex LLM
                "synonym_model": "mistral-large",  # LLM model for synonym generation
                "synonym_max_count": 4,  # Maximum synonyms per table/column
                "generate_column_synonyms": True,  # Also generate column-level synonyms
            },
            "logging": {"level": "INFO", "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"},
        }

    def _find_config_file(self) -> Optional[Path]:
        """
        Find sst_config.yaml in:
        1. Current directory
        2. Parent directories (up to 3 levels)
        3. User home directory
        """
        # Check current directory and parents
        current = Path.cwd()
        for _ in range(4):  # Check up to 3 parent directories
            # Check both .yaml and .yml extensions
            for ext in ["yaml", "yml"]:
                config_path = current / f"sst_config.{ext}"
                if config_path.exists():
                    return config_path

                # Also check hidden variant
                config_path = current / f".sst_config.{ext}"
                if config_path.exists():
                    return config_path

            if current.parent == current:
                break
            current = current.parent

        # Check home directory (both extensions)
        for ext in ["yaml", "yml"]:
            home_config = Path.home() / f".sst_config.{ext}"
            if home_config.exists():
                return home_config

        return None

    def _load_yaml_config(self, config_file: Path) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_file, "r") as f:
                config = yaml.safe_load(f) or {}
            return config
        except Exception as e:
            # Log warning but don't fail
            print(f"Warning: Could not load config file {config_file}: {e}")
            return {}

    def _merge_config(self, base: Dict, override: Dict):
        """Recursively merge override config into base config."""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_config(base[key], value)
            else:
                base[key] = value

    def _load_env_overrides(self):
        """
        Load overrides from environment variables.

        Note: Only credentials and per-developer settings should be in .env
        Project configuration (repository, validation rules) should be in sst_config.yml
        """
        # Snowflake config (if needed)
        if os.getenv("SNOWFLAKE_ACCOUNT"):
            if "snowflake" not in self._config:
                self._config["snowflake"] = {}
            self._config["snowflake"]["account"] = os.getenv("SNOWFLAKE_ACCOUNT")

        # Logging config
        if os.getenv("LOG_LEVEL"):
            self._config["logging"]["level"] = os.getenv("LOG_LEVEL")

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get config value using dot notation.

        Args:
            key_path: Dot-separated path (e.g., 'project.semantic_models_dir')
            default: Default value if key not found

        Returns:
            Configuration value or default

        Examples:
            >>> config = Config()
            >>> config.get('project.semantic_models_dir')
            'snowflake_semantic_models'
            >>> config.get('validation.strict', False)
            False
        """
        keys = key_path.split(".")
        value = self._config

        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default

        return value

    def get_exclude_dirs(self) -> list:
        """Get validation exclude directories."""
        return self.get("validation.exclude_dirs", [])

    def is_strict_mode(self) -> bool:
        """Check if strict validation mode is enabled."""
        return self.get("validation.strict", False)

    def is_verbose(self) -> bool:
        """Check if verbose output is enabled."""
        return self.get("validation.verbose", False)

    def get_enrichment_distinct_limit(self) -> int:
        """Get the number of distinct values to fetch during enrichment."""
        return self.get("enrichment.distinct_limit", 25)

    def get_enrichment_display_limit(self) -> int:
        """Get the number of sample values to display in YAML files."""
        return self.get("enrichment.sample_values_display_limit", 10)

    def get_synonym_model(self) -> str:
        """Get LLM model for synonym generation (default: openai-gpt-4.1)."""
        return self.get("enrichment.synonym_model", "openai-gpt-4.1")

    def get_synonym_max_count(self) -> int:
        """Get maximum number of synonyms to generate (default: 4)."""
        return self.get("enrichment.synonym_max_count", 4)

    def reload(self):
        """Reload configuration (useful for testing)."""
        self._load_config()


def get_config() -> Config:
    """
    Get the global configuration instance.

    Returns:
        Config singleton instance

    Examples:
        >>> from snowflake_semantic_tools.shared.config import get_config
        >>> config = get_config()
        >>> repo_url = config.get_repository_url()
    """
    return Config()
