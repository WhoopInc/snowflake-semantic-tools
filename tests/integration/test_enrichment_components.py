#!/usr/bin/env python3
"""
Integration Tests for Modular Enrichment Components

Tests the component flag system and synonym generation integration.
"""

import pytest

from snowflake_semantic_tools.interfaces.cli.commands.enrich import _determine_components


class TestComponentDetermination:
    """Test component determination logic."""

    def test_no_flags_default_behavior(self):
        """Test default behavior (backward compatible)."""
        components = _determine_components(
            enrich_all=False,
            synonyms=False,
            column_types=False,
            data_types=False,
            sample_values=False,
            detect_enums=False,
            primary_keys=False,
            table_synonyms=False,
            column_synonyms=False,
        )

        # Should include all standard components
        assert "column-types" in components
        assert "data-types" in components
        assert "sample-values" in components
        assert "detect-enums" in components
        assert "primary-keys" in components

        # Should NOT include synonyms (backward compatible)
        assert "table-synonyms" not in components
        assert "column-synonyms" not in components

    def test_all_flag_includes_everything(self):
        """Test --all includes all components including synonyms."""
        components = _determine_components(
            enrich_all=True,
            synonyms=False,
            column_types=False,
            data_types=False,
            sample_values=False,
            detect_enums=False,
            primary_keys=False,
            table_synonyms=False,
            column_synonyms=False,
        )

        # Should include EVERYTHING
        assert "column-types" in components
        assert "data-types" in components
        assert "sample-values" in components
        assert "detect-enums" in components
        assert "primary-keys" in components
        assert "table-synonyms" in components  # Included with --all
        assert "column-synonyms" in components  # Included with --all

    def test_synonyms_flag_shorthand(self):
        """Test --synonyms includes both table and column synonyms."""
        components = _determine_components(
            enrich_all=False,
            synonyms=True,
            column_types=False,
            data_types=False,
            sample_values=False,
            detect_enums=False,
            primary_keys=False,
            table_synonyms=False,
            column_synonyms=False,
        )

        # Should include both synonym types
        assert "table-synonyms" in components
        assert "column-synonyms" in components

        # Should NOT include other components (explicit flag override)
        assert "sample-values" not in components
        assert "column-types" not in components

    def test_individual_flags_explicit_control(self):
        """Test individual flags give explicit control."""
        components = _determine_components(
            enrich_all=False,
            synonyms=False,
            column_types=True,
            data_types=True,
            sample_values=False,
            detect_enums=False,
            primary_keys=False,
            table_synonyms=True,
            column_synonyms=False,
        )

        # Should include ONLY what was specified
        assert "column-types" in components
        assert "data-types" in components
        assert "table-synonyms" in components

        # Should NOT include others
        assert "sample-values" not in components
        assert "column-synonyms" not in components

    def test_table_synonyms_only(self):
        """Test --table-synonyms without --column-synonyms."""
        components = _determine_components(
            enrich_all=False,
            synonyms=False,
            column_types=False,
            data_types=False,
            sample_values=False,
            detect_enums=False,
            primary_keys=False,
            table_synonyms=True,
            column_synonyms=False,
        )

        assert "table-synonyms" in components
        assert "column-synonyms" not in components

    def test_column_synonyms_only(self):
        """Test --column-synonyms without --table-synonyms."""
        components = _determine_components(
            enrich_all=False,
            synonyms=False,
            column_types=False,
            data_types=False,
            sample_values=False,
            detect_enums=False,
            primary_keys=False,
            table_synonyms=False,
            column_synonyms=True,
        )

        assert "column-synonyms" in components
        assert "table-synonyms" not in components

    def test_all_plus_explicit_flags(self):
        """Test that --all takes precedence."""
        components = _determine_components(
            enrich_all=True,
            synonyms=False,
            column_types=True,
            data_types=False,
            sample_values=False,
            detect_enums=False,
            primary_keys=False,
            table_synonyms=False,
            column_synonyms=False,
        )

        # --all should override and include everything
        assert len(components) == 7  # All 7 components

    def test_sample_values_with_synonyms(self):
        """Test common use case: refresh samples and add synonyms."""
        components = _determine_components(
            enrich_all=False,
            synonyms=True,
            column_types=False,
            data_types=False,
            sample_values=True,
            detect_enums=False,
            primary_keys=False,
            table_synonyms=False,
            column_synonyms=False,
        )

        # Should include samples and both synonyms
        assert "sample-values" in components
        assert "table-synonyms" in components
        assert "column-synonyms" in components
        assert len(components) == 3  # Only these 3


class TestEnrichmentComponentScenarios:
    """Test real-world component usage scenarios."""

    def test_scenario_just_synonyms_fast(self):
        """Scenario: Add synonyms to already-enriched models (FAST)."""
        components = _determine_components(
            enrich_all=False,
            synonyms=True,
            column_types=False,
            data_types=False,
            sample_values=False,
            detect_enums=False,
            primary_keys=False,
            table_synonyms=False,
            column_synonyms=False,
        )

        # Should be just synonyms (no data queries)
        assert components == ["table-synonyms", "column-synonyms"]

    def test_scenario_full_enrichment_with_synonyms(self):
        """Scenario: New table, enrich everything."""
        components = _determine_components(
            enrich_all=True,
            synonyms=False,
            column_types=False,
            data_types=False,
            sample_values=False,
            detect_enums=False,
            primary_keys=False,
            table_synonyms=False,
            column_synonyms=False,
        )

        # Should include all 7 components
        expected = [
            "column-types",
            "data-types",
            "sample-values",
            "detect-enums",
            "primary-keys",
            "table-synonyms",
            "column-synonyms",
        ]
        assert set(components) == set(expected)

    def test_scenario_refresh_data_only(self):
        """Scenario: Refresh sample values and enums."""
        components = _determine_components(
            enrich_all=False,
            synonyms=False,
            column_types=False,
            data_types=False,
            sample_values=True,
            detect_enums=True,
            primary_keys=False,
            table_synonyms=False,
            column_synonyms=False,
        )

        assert components == ["sample-values", "detect-enums"]

    def test_scenario_fast_enrichment(self):
        """Scenario: Types and synonyms, skip expensive data queries."""
        components = _determine_components(
            enrich_all=False,
            synonyms=True,
            column_types=True,
            data_types=True,
            sample_values=False,
            detect_enums=False,
            primary_keys=False,
            table_synonyms=False,
            column_synonyms=False,
        )

        # Should include types and synonyms
        assert "column-types" in components
        assert "data-types" in components
        assert "table-synonyms" in components
        assert "column-synonyms" in components

        # Should NOT include expensive parts
        assert "sample-values" not in components
