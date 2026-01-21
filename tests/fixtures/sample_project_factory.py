"""
Factory for creating test project variants from the embedded sample project.

This module provides utilities for creating clean copies and variants of the
sample dbt+SST project for integration testing.
"""

import shutil
from pathlib import Path

import yaml


class SampleProjectFactory:
    """Factory for creating test project variants from the embedded sample project."""

    BASE_PROJECT = Path(__file__).parent / "sample_project"

    @classmethod
    def create(cls, tmp_path: Path, name: str = "test_project") -> Path:
        """
        Create a clean copy of the sample project.

        Args:
            tmp_path: Temporary directory path (typically from pytest tmp_path fixture)
            name: Name for the project directory

        Returns:
            Path to the created project directory
        """
        project_dir = tmp_path / name
        shutil.copytree(cls.BASE_PROJECT, project_dir)
        return project_dir

    @classmethod
    def create_without_sst(cls, tmp_path: Path, name: str = "test_project") -> Path:
        """
        Create project without SST initialization (for init wizard tests).

        This creates a valid dbt project but without:
        - sst_config.yaml
        - snowflake_semantic_models directory

        Args:
            tmp_path: Temporary directory path
            name: Name for the project directory

        Returns:
            Path to the created project directory
        """
        project_dir = cls.create(tmp_path, name)
        shutil.rmtree(project_dir / "snowflake_semantic_models")
        (project_dir / "sst_config.yaml").unlink()
        return project_dir

    @classmethod
    def create_without_manifest(cls, tmp_path: Path, name: str = "test_project") -> Path:
        """
        Create project without manifest (for compile-required tests).

        This creates a complete project but removes the manifest.json file,
        useful for testing scenarios where dbt compile hasn't been run.

        Args:
            tmp_path: Temporary directory path
            name: Name for the project directory

        Returns:
            Path to the created project directory
        """
        project_dir = cls.create(tmp_path, name)
        manifest_path = project_dir / "target" / "manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()
        return project_dir

    @classmethod
    def create_with_invalid_metrics(cls, tmp_path: Path, name: str = "test_project") -> Path:
        """
        Create project with intentionally invalid metrics.

        Useful for testing validation error detection.

        Args:
            tmp_path: Temporary directory path
            name: Name for the project directory

        Returns:
            Path to the created project directory
        """
        project_dir = cls.create(tmp_path, name)
        invalid_metrics = project_dir / "snowflake_semantic_models" / "metrics" / "invalid.yml"
        invalid_metrics.write_text("""snowflake_metrics:
  - name: broken_metric
    description: "Missing required fields - no expr"
""")
        return project_dir

    @classmethod
    def create_with_missing_table_reference(cls, tmp_path: Path, name: str = "test_project") -> Path:
        """
        Create project with metric referencing non-existent table.

        Useful for testing template resolution error handling.

        Args:
            tmp_path: Temporary directory path
            name: Name for the project directory

        Returns:
            Path to the created project directory
        """
        project_dir = cls.create(tmp_path, name)
        invalid_ref = project_dir / "snowflake_semantic_models" / "metrics" / "invalid_ref.yml"
        invalid_ref.write_text("""snowflake_metrics:
  - name: bad_reference
    description: "References non-existent table"
    tables:
      - "{{ table('nonexistent_model') }}"
    expr: "COUNT(*)"
""")
        return project_dir

    @classmethod
    def create_unenriched(cls, tmp_path: Path, name: str = "test_project") -> Path:
        """
        Create project with models that have no SST metadata (for enrich tests).

        This strips all SST metadata from model YAML files, useful for testing
        the enrichment workflow.

        Args:
            tmp_path: Temporary directory path
            name: Name for the project directory

        Returns:
            Path to the created project directory
        """
        project_dir = cls.create(tmp_path, name)
        # Strip SST metadata from model YAML files
        for yml_file in (project_dir / "models").rglob("*.yml"):
            if yml_file.name != "__sources.yml":
                cls._strip_sst_metadata(yml_file)
        return project_dir

    @classmethod
    def create_with_profiles(cls, tmp_path: Path, name: str = "test_project") -> Path:
        """
        Create project with a working profiles.yml (using template values).

        Args:
            tmp_path: Temporary directory path
            name: Name for the project directory

        Returns:
            Path to the created project directory
        """
        project_dir = cls.create(tmp_path, name)
        profiles_content = """sample_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: test.us-east-1
      user: test_user
      authenticator: externalbrowser
      role: TEST_ROLE
      warehouse: TEST_WH
      database: SAMPLE_DATABASE
      schema: DEV
"""
        (project_dir / "profiles.yml").write_text(profiles_content)
        return project_dir

    @staticmethod
    def _strip_sst_metadata(yml_file: Path) -> None:
        """
        Remove config.meta.sst blocks from a YAML file.

        Args:
            yml_file: Path to the YAML file to modify
        """
        with open(yml_file) as f:
            data = yaml.safe_load(f)

        if data is None:
            return

        if "models" in data:
            for model in data["models"]:
                # Remove model-level config
                if "config" in model:
                    model.pop("config", None)
                # Remove column-level config
                for col in model.get("columns", []):
                    col.pop("config", None)

        with open(yml_file, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    @classmethod
    def get_base_project_path(cls) -> Path:
        """
        Get the path to the base sample project.

        Returns:
            Path to the sample_project directory
        """
        return cls.BASE_PROJECT
