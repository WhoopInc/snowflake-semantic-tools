#!/usr/bin/env python3
"""
Unit tests for prompt_loader module.

Tests prompt template loading and rendering functionality.
"""

import pytest

from snowflake_semantic_tools.core.enrichment.prompt_loader import clear_cache, load_prompt, render_prompt


class TestLoadPrompt:
    """Tests for load_prompt function."""

    def test_load_table_synonyms_prompt(self):
        """Should load table_synonyms.md template."""
        template = load_prompt("table_synonyms")
        assert template is not None
        assert len(template) > 0
        assert "{table_name}" in template
        assert "{readable_name}" in template
        assert "{max_synonyms}" in template

    def test_load_column_synonyms_prompt(self):
        """Should load column_synonyms.md template."""
        template = load_prompt("column_synonyms")
        assert template is not None
        assert len(template) > 0
        assert "{table_name}" in template
        assert "{columns_text}" in template

    def test_load_nonexistent_prompt_raises_error(self):
        """Should raise FileNotFoundError for missing template."""
        with pytest.raises(FileNotFoundError) as exc_info:
            load_prompt("nonexistent_template")
        assert "Prompt template not found" in str(exc_info.value)

    def test_prompt_caching(self):
        """Should cache loaded prompts."""
        clear_cache()

        # Load twice - second call should use cache
        template1 = load_prompt("table_synonyms")
        template2 = load_prompt("table_synonyms")

        assert template1 is template2  # Same object reference due to caching


class TestRenderPrompt:
    """Tests for render_prompt function."""

    def test_render_table_synonyms_prompt(self):
        """Should render table_synonyms template with variables."""
        rendered = render_prompt(
            "table_synonyms",
            table_name="INT_ORDERS",
            readable_name="orders",
            description="Customer order transactions",
            full_context="columns:\n  - order_id\n  - customer_id",
            max_synonyms=4,
            avoid_synonyms_section="",
        )

        assert "INT_ORDERS" in rendered
        assert "orders" in rendered
        assert "Customer order transactions" in rendered
        assert "4" in rendered

    def test_render_with_avoid_synonyms_section(self):
        """Should include avoid synonyms section when provided."""
        avoid_section = """AVOID THESE SYNONYMS:
- "order history"
- "purchase records"
"""
        rendered = render_prompt(
            "table_synonyms",
            table_name="INT_ORDERS",
            readable_name="orders",
            description="Orders data",
            full_context="...",
            max_synonyms=4,
            avoid_synonyms_section=avoid_section,
        )

        assert "AVOID THESE SYNONYMS" in rendered
        assert "order history" in rendered
        assert "purchase records" in rendered

    def test_render_column_synonyms_prompt(self):
        """Should render column_synonyms template with variables."""
        rendered = render_prompt(
            "column_synonyms",
            table_name="CUSTOMERS",
            table_description="Customer master data",
            columns_text="  - customer_id: Unique ID\n  - name: Customer name",
            yaml_context="Not available",
            max_synonyms=4,
        )

        assert "CUSTOMERS" in rendered
        assert "Customer master data" in rendered
        assert "customer_id" in rendered

    def test_render_missing_variable_raises_error(self):
        """Should raise KeyError when required variable is missing."""
        with pytest.raises(KeyError):
            render_prompt(
                "table_synonyms",
                table_name="TEST",
                # Missing: readable_name, description, full_context, max_synonyms, avoid_synonyms_section
            )

    def test_render_nonexistent_template_raises_error(self):
        """Should raise FileNotFoundError for missing template."""
        with pytest.raises(FileNotFoundError):
            render_prompt("nonexistent", variable="value")


class TestClearCache:
    """Tests for clear_cache function."""

    def test_clear_cache_resets_caching(self):
        """Should clear the prompt cache."""
        # Load a prompt to populate cache
        load_prompt("table_synonyms")

        # Clear cache
        clear_cache()

        # Verify cache was cleared by checking cache_info
        info = load_prompt.cache_info()
        assert info.currsize == 0
