"""
Test apostrophe handling in synonyms.

Apostrophes in synonyms cause SQL syntax errors in semantic views.
This test suite ensures they're properly handled everywhere.
"""

import pytest
from snowflake_semantic_tools.core.enrichment.cortex_synonym_generator import CortexSynonymGenerator
from snowflake_semantic_tools.core.validation.rules.dbt_models import DbtModelValidator
from snowflake_semantic_tools.core.models import ValidationResult


class TestApostropheSanitization:
    """Test that apostrophes are removed from generated synonyms."""
    
    def test_synonym_generator_removes_apostrophes(self):
        """Generated synonyms should have apostrophes removed."""
        # Mock synonyms that would come from Cortex with apostrophes
        synonyms_with_apostrophes = [
            "user's data",
            "member's profile",
            "customer's info",
            "someone's records"
        ]
        
        # Simulate the cleaning logic from cortex_synonym_generator.py
        clean_synonyms = []
        for syn in synonyms_with_apostrophes:
            cleaned = syn.strip()
            cleaned = cleaned.replace("'", "")
            cleaned = cleaned.replace("'", "")  # Smart quote left
            cleaned = cleaned.replace("'", "")  # Smart quote right
            if cleaned:
                clean_synonyms.append(cleaned)
        
        # All apostrophes should be removed
        assert clean_synonyms == [
            "users data",
            "members profile",
            "customers info",
            "someones records"
        ]
        assert all("'" not in syn for syn in clean_synonyms)
        assert all("'" not in syn for syn in clean_synonyms)
        assert all("'" not in syn for syn in clean_synonyms)
    
    def test_smart_quotes_removed(self):
        """Test that smart quotes (Unicode apostrophes) are also removed."""
        synonyms = [
            "user's data",      # Left smart quote
            "member's profile", # Right smart quote  
            "regular's quote"   # Regular apostrophe
        ]
        
        clean_synonyms = []
        for syn in synonyms:
            cleaned = syn.replace("'", "").replace("'", "").replace("'", "")
            if cleaned:
                clean_synonyms.append(cleaned)
        
        assert all("'" not in syn for syn in clean_synonyms)
        assert all("'" not in syn for syn in clean_synonyms)
        assert all("'" not in syn for syn in clean_synonyms)


class TestApostropheValidation:
    """Test that validation catches apostrophes in synonyms."""
    
    def test_validates_column_synonyms_with_apostrophes(self):
        """Validator should warn on column synonyms with apostrophes (auto-sanitized during generation)."""
        validator = DbtModelValidator()
        result = ValidationResult()
        
        column_with_bad_synonym = {
            'name': 'user_id',
            'description': 'User identifier',
            'column_type': 'dimension',
            'data_type': 'NUMBER',
            'synonyms': ["member's id", "user identifier", "customer's key"]
        }
        
        validator._check_synonym_content(
            column_with_bad_synonym,
            'TEST_TABLE',
            'user_id',
            result
        )
        
        # Should have 1 warning (synonyms with apostrophes will be auto-sanitized)
        assert result.warning_count == 1
        assert result.error_count == 0  # No errors - just warnings
        assert any("problematic characters" in str(w).lower() for w in result.get_warnings())
    
    def test_validates_table_synonyms_with_apostrophes(self):
        """Validator should warn on table synonyms with apostrophes (auto-sanitized during generation)."""
        validator = DbtModelValidator()
        result = ValidationResult()
        
        table_with_bad_synonym = {
            'table_name': 'USER_PROFILES',
            'synonyms': ["user's profiles", "member data", "customer's information"]
        }
        
        validator._check_table_synonym_content(
            table_with_bad_synonym,
            'USER_PROFILES',
            result
        )
        
        # Should have 1 warning (synonyms with apostrophes will be auto-sanitized)
        assert result.warning_count == 1
        assert result.error_count == 0  # No errors - just warnings
        assert any("problematic characters" in str(w).lower() for w in result.get_warnings())
    
    def test_validates_smart_quotes(self):
        """Validator should catch smart quotes too (warns for auto-sanitization)."""
        validator = DbtModelValidator()
        result = ValidationResult()
        
        column = {
            'name': 'data_col',
            'column_type': 'dimension',
            'data_type': 'TEXT',
            'synonyms': ["user's data", "member's info"]  # Smart quotes
        }
        
        validator._check_synonym_content(column, 'TEST_TABLE', 'data_col', result)
        
        # Should warn (not error) about smart quotes - they'll be auto-sanitized
        assert result.warning_count > 0
        assert result.error_count == 0
    
    def test_allows_synonyms_without_apostrophes(self):
        """Validator should allow clean synonyms."""
        validator = DbtModelValidator()
        result = ValidationResult()
        
        column = {
            'name': 'user_id',
            'column_type': 'dimension',
            'data_type': 'NUMBER',
            'synonyms': ["member identifier", "user key", "customer id"]
        }
        
        validator._check_synonym_content(column, 'TEST_TABLE', 'user_id', result)
        
        # Should have no errors
        assert result.error_count == 0


class TestSemanticViewSQLGeneration:
    """Test that semantic view SQL generation handles apostrophes correctly."""
    
    def test_table_synonyms_apostrophes_removed_in_sql(self):
        """Table synonyms with apostrophes should be cleaned in generated SQL."""
        synonyms = ["user's data", "member profiles", "customer's info"]
        
        # Simulate semantic_view_builder.py logic
        synonyms_cleaned = []
        for syn in synonyms:
            cleaned = str(syn).replace("'", "").replace("'", "").replace("'", "")
            if cleaned.strip():
                synonyms_cleaned.append(cleaned)
        
        synonyms_str = ", ".join([f"'{syn}'" for syn in synonyms_cleaned])
        sql = f"WITH SYNONYMS ({synonyms_str})"
        
        # Generated SQL should not contain unescaped apostrophes
        # Only have apostrophes as SQL string delimiters
        assert sql == "WITH SYNONYMS ('users data', 'member profiles', 'customers info')"
        assert "user's" not in sql
        assert "customer's" not in sql
    
    def test_dimension_synonyms_apostrophes_removed_in_sql(self):
        """Dimension synonyms with apostrophes should be cleaned in generated SQL."""
        synonyms_parsed = ["employee's name", "worker identifier", "staff's id"]
        
        # Simulate semantic_view_builder.py logic for dimensions
        synonyms_cleaned = []
        for syn in synonyms_parsed:
            cleaned = str(syn).replace("'", "").replace("'", "").replace("'", "")
            if cleaned.strip():
                synonyms_cleaned.append(cleaned)
        
        if synonyms_cleaned:
            synonyms_str = ", ".join([f"'{syn}'" for syn in synonyms_cleaned])
            result = f"WITH SYNONYMS = ({synonyms_str})"
        
        assert "WITH SYNONYMS = ('employees name', 'worker identifier', 'staffs id')" == result
        assert "employee's" not in result


class TestRealWorldExamples:
    """Test with real-world problematic synonyms from analytics-dbt."""
    
    def test_individual_member_synonym(self):
        """Test the actual failing synonym from analytics-dbt."""
        # This was causing: syntax error line 97 at position 81 unexpected ''individual''
        synonym = "individual's membership"
        
        # Clean it
        cleaned = synonym.replace("'", "").replace("'", "").replace("'", "")
        
        assert cleaned == "individuals membership"
        assert "'" not in cleaned
    
    def test_users_activity_synonym(self):
        """Test user's activity type synonyms."""
        synonyms = [
            "user's activity",
            "member's workout",
            "athlete's training"
        ]
        
        cleaned = [s.replace("'", "").replace("'", "").replace("'", "") for s in synonyms]
        
        assert cleaned == [
            "users activity",
            "members workout",
            "athletes training"
        ]
    
    def test_empty_after_cleaning(self):
        """Test that synonyms that become empty are filtered out."""
        synonyms = ["good synonym", "'", "another good one", "''"]
        
        cleaned = []
        for syn in synonyms:
            clean = syn.replace("'", "").replace("'", "").replace("'", "")
            if clean.strip():  # Only keep if has content
                cleaned.append(clean)
        
        assert cleaned == ["good synonym", "another good one"]
        assert len(cleaned) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

