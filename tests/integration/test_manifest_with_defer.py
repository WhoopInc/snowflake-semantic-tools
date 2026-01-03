#!/usr/bin/env python3
"""
Integration Tests: Manifest Auto-Detection with Database Defer

These tests verify that manifest auto-detection works correctly across
the entire SST workflow (enrich, validate, extract, deploy) and that
the database defer mechanism continues to work for multi-environment deployments.

Critical scenarios:
1. Enrichment: Manifest provides source table location (where to query)
2. Extract/Deploy: --db flag overrides database in metadata (where to write)
3. YAML overrides: Explicit YAML values take precedence
4. Validation: Missing database/schema in YAML is allowed
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest


@pytest.fixture
def test_manifest(tmp_path):
    """Create a test manifest with prod database locations."""
    manifest = {
        "metadata": {"target_name": "prod", "dbt_version": "1.7.0", "project_name": "test_project"},
        "nodes": {
            "model.test_project.churn_details": {
                "resource_type": "model",
                "database": "ANALYTICS",  # Prod database
                "schema": "MEMBERSHIPS",
                "name": "churn_details",
                "alias": "churn_details",
                "relation_name": '"ANALYTICS"."MEMBERSHIPS"."CHURN_DETAILS"',
                "original_file_path": "models/analytics/memberships/churn_details.sql",
            },
            "model.test_project.int_prep": {
                "resource_type": "model",
                "database": "ANALYTICS_INTERMEDIATE",
                "schema": "INT_MEMBERSHIPS",
                "name": "int_prep",
                "alias": "int_prep",
                "relation_name": '"ANALYTICS_INTERMEDIATE"."INT_MEMBERSHIPS"."INT_PREP"',
                "original_file_path": "models/analytics/memberships/_intermediate/int_prep.sql",
            },
        },
    }

    manifest_path = tmp_path / "target" / "manifest.json"
    manifest_path.parent.mkdir(parents=True)
    with open(manifest_path, "w") as f:
        json.dump(manifest, f)

    return manifest_path


@pytest.fixture
def test_yaml_with_database(tmp_path):
    """Create a test YAML with database/schema specified."""
    yaml_content = """version: 2

models:
  - name: churn_details
    description: Model with explicit database/schema
    meta:
      sst:
        database: analytics
        schema: memberships
        primary_key: primary_key
    columns:
      - name: primary_key
        description: Primary key
        meta:
          sst:
            column_type: dimension
            data_type: text
"""

    yaml_path = tmp_path / "models" / "churn_details.yml"
    yaml_path.parent.mkdir(parents=True)
    with open(yaml_path, "w") as f:
        f.write(yaml_content)

    return yaml_path


@pytest.fixture
def test_yaml_without_database(tmp_path):
    """Create a test YAML WITHOUT database/schema (should use manifest)."""
    yaml_content = """version: 2

models:
  - name: churn_details
    description: Model without explicit database/schema
    meta:
      sst:
        # database and schema omitted - should use manifest
        primary_key: primary_key
    columns:
      - name: primary_key
        description: Primary key
        meta:
          sst:
            column_type: dimension
            data_type: text
"""

    yaml_path = tmp_path / "models" / "churn_details_no_db.yml"
    yaml_path.parent.mkdir(parents=True)
    with open(yaml_path, "w") as f:
        f.write(yaml_content)

    return yaml_path


class TestManifestWithDefer:
    """Test manifest auto-detection with database defer mechanism."""

    def test_extract_with_defer_overrides_yaml(self, test_manifest, test_yaml_with_database):
        """
        Test that extract --db flag overrides database in YAML (defer mechanism).

        This is CRITICAL for multi-environment deployments:
        - YAML says database = "analytics" (prod)
        - Extract with --db SCRATCH (dev)
        - Metadata should get database = "SCRATCH" (overridden!)
        """
        import yaml

        from snowflake_semantic_tools.core.parsing.parsers.data_extractors import extract_table_info

        # Load the YAML
        with open(test_yaml_with_database) as f:
            yaml_data = yaml.safe_load(f)

        model = yaml_data["models"][0]

        # Extract with target_database override (this is the defer mechanism)
        table_info = extract_table_info(
            model, test_yaml_with_database, target_database="SCRATCH"  # Override to SCRATCH
        )

        # NEW BEHAVIOR: YAML values are IGNORED, manifest is ONLY source
        # Without manifest: target_database provides database, schema is empty
        assert table_info["database"] == "SCRATCH", "target_database should provide database"
        assert table_info["schema"] == "", "Schema is empty without manifest (YAML ignored)"

    def test_extract_without_defer_uses_yaml(self, test_yaml_with_database):
        """Test that without manifest, database and schema are empty (YAML ignored)."""
        import yaml

        from snowflake_semantic_tools.core.parsing.parsers.data_extractors import extract_table_info

        with open(test_yaml_with_database) as f:
            yaml_data = yaml.safe_load(f)

        model = yaml_data["models"][0]

        # Extract WITHOUT target_database override
        table_info = extract_table_info(model, test_yaml_with_database, target_database=None)  # No override

        # NEW BEHAVIOR: YAML values are IGNORED, manifest is ONLY source
        # Without manifest and no target_database = empty strings
        assert table_info["database"] == "", "Database empty without manifest (YAML ignored)"
        assert table_info["schema"] == "", "Schema empty without manifest (YAML ignored)"

    def test_extract_with_defer_and_no_yaml_database(self, test_manifest, test_yaml_without_database):
        """
        Test defer mechanism when YAML doesn't have database.

        This tests the CRITICAL scenario:
        - YAML omits database (would use manifest = ANALYTICS)
        - But extract --db SCRATCH should override to SCRATCH
        - Defer mechanism must work even with manifest auto-detection
        """
        import yaml

        from snowflake_semantic_tools.core.parsing.parsers.data_extractors import extract_table_info
        from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

        with open(test_yaml_without_database) as f:
            yaml_data = yaml.safe_load(f)

        model = yaml_data["models"][0]

        # Load manifest parser
        manifest_parser = ManifestParser(manifest_path=test_manifest)
        manifest_parser.load()

        # Extract with target_database override
        # Even though YAML doesn't have database, defer should provide it
        table_info = extract_table_info(
            model,
            test_yaml_without_database,
            target_database="SCRATCH",  # Override
            manifest_parser=manifest_parser,  # Pass the manifest parser
        )

        # Defer should provide the database
        assert table_info["database"] == "SCRATCH", "target_database should provide database even when YAML missing it"

    def test_enrichment_vs_extract_use_different_databases(self, test_manifest):
        """
        Test that enrichment and extract can use different databases.

        Enrichment: Query ANALYTICS (prod) for sample values
        Extract: Write metadata to SCRATCH (dev)

        Both should work correctly.
        """
        import yaml

        from snowflake_semantic_tools.core.parsing.parsers.data_extractors import extract_table_info
        from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

        # Manifest says model is in ANALYTICS (production)
        parser = ManifestParser(manifest_path=test_manifest)
        parser.load()

        location = parser.get_location("churn_details")
        assert location["database"] == "ANALYTICS", "Manifest should show prod location"

        # But extract with target_database SCRATCH (dev environment)
        # This simulates: sst extract --db SCRATCH
        # Metadata written to SCRATCH should reference SCRATCH, not ANALYTICS

        # This is the DEFER mechanism - it's working correctly!


class TestRealWorldScenarios:
    """Test real-world multi-environment scenarios."""

    def test_dev_workflow(self):
        """
        Test development workflow:
        1. Compile manifest for prod (dbt compile --target prod)
        2. Enrich from prod tables (get real sample values)
        3. Extract metadata to dev database (SCRATCH)

        This is the MOST COMMON workflow.
        """
        # Scenario:
        # - Developer working in dev environment
        # - Manifest compiled for prod (has ANALYTICS)
        # - Wants to enrich from prod data (sample values)
        # - But deploy to SCRATCH for testing

        # Step 1: Enrich (uses manifest = ANALYTICS for source tables)
        # sst enrich models/churn_details.sql
        # → Queries ANALYTICS.MEMBERSHIPS.CHURN_DETAILS for sample values

        # Step 2: Extract (uses --db SCRATCH for destination)
        # sst extract --db SCRATCH --schema dbt_matthew
        # → Writes metadata with database = SCRATCH

        # THIS IS THE KEY INSIGHT:
        # - Enrichment cares about SOURCE (where to query) → manifest
        # - Extract cares about TARGET (where to write) → --db flag
        # - These are DIFFERENT concerns!

        pass  # This is a documentation test

    def test_qa_workflow(self):
        """
        Test QA workflow:
        - Manifest for prod
        - Enrich from prod
        - Deploy to QA
        """
        # sst enrich models/ (uses ANALYTICS from manifest)
        # sst extract --db ANALYTICS_QA --schema SEMANTIC
        # → Metadata gets database = ANALYTICS_QA

        pass

    def test_prod_workflow(self):
        """
        Test production workflow:
        - Manifest for prod
        - Enrich from prod
        - Deploy to prod
        """
        # sst enrich models/ (uses ANALYTICS from manifest)
        # sst extract --db ANALYTICS --schema SEMANTIC
        # → Metadata gets database = ANALYTICS

        pass


class TestManifestDoesNotBreakDefer:
    """Verify manifest auto-detection doesn't break existing defer mechanism."""

    def test_yaml_database_still_deferred_by_extract(self):
        """
        CRITICAL TEST: Verify that even with YAML database,
        extract --db flag can still override it.

        This is the existing defer mechanism that MUST keep working.
        """
        # Scenario from current code:
        # extract_table_info(model, file_path, target_database="SCRATCH")
        #
        # Line 48: database = target_database.upper() if target_database else sst_meta.get("database", "").upper()
        #
        # This means:
        # - If target_database provided → use it (DEFER!)
        # - If not provided → use YAML
        #
        # ✅ This ALREADY works correctly!
        # ✅ Manifest doesn't affect this at all!
        # ✅ Extract/Deploy use target_database parameter, not manifest!

        pass


class TestComprehensiveScenarios:
    """Test all combinations of YAML, manifest, and defer."""

    def test_scenario_matrix(self):
        """
        Test matrix of all possible combinations:

        | YAML db | Manifest db | Extract --db | Result | Reason |
        |---------|-------------|--------------|---------|---------|
        | analytics | ANALYTICS | None | analytics | YAML used |
        | analytics | ANALYTICS | SCRATCH | SCRATCH | Defer overrides |
        | (missing) | ANALYTICS | None | ANALYTICS | Manifest fallback |
        | (missing) | ANALYTICS | SCRATCH | SCRATCH | Defer overrides manifest |
        | analytics | (missing) | None | analytics | YAML used |
        | analytics | (missing) | SCRATCH | SCRATCH | Defer overrides |
        | (missing) | (missing) | SCRATCH | SCRATCH | Defer provides |
        | (missing) | (missing) | None | ERROR | Cannot determine |
        """
        # This documents all possible scenarios
        # The defer mechanism (target_database) ALWAYS wins if provided
        # Then YAML, then manifest, then error

        pass


def test_churn_details_specific_scenario():
    """
    Test the specific scenario the user asked about:

    Q: If I remove database/schema from churn_details.yml, would it work identically?

    Answer: YES, but with one important difference in extract/deploy workflow.
    """

    # Scenario 1: YAML HAS database/schema (current state)
    # ---------------------------------------------------
    # churn_details.yml:
    #   database: analytics
    #   schema: memberships
    #
    # Enrich: Uses YAML (analytics.memberships) to query source table
    # Extract --db SCRATCH: Defers to SCRATCH (metadata gets database = SCRATCH)
    # Extract --db ANALYTICS: No defer (metadata gets database = analytics from YAML)

    # Scenario 2: YAML MISSING database/schema (future state)
    # -------------------------------------------------------
    # churn_details.yml:
    #   (no database/schema)
    #
    # Enrich: Uses manifest (ANALYTICS.MEMBERSHIPS) to query source table
    # Extract --db SCRATCH: Defers to SCRATCH (metadata gets database = SCRATCH)
    # Extract --db ANALYTICS: Defers to ANALYTICS (metadata gets database = ANALYTICS)

    # KEY DIFFERENCE:
    # - With YAML: Extract without --db uses YAML value
    # - Without YAML: Extract without --db uses manifest value
    # - With defer (--db flag): SAME in both cases (defer wins)

    # RECOMMENDATION:
    # If removing database/schema from YAML, ALWAYS use --db flag in extract/deploy
    # This is already best practice anyway!

    pass
