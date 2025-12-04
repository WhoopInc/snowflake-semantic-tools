"""
Semantic Validator

Central orchestrator for comprehensive semantic model validation.

Coordinates multiple validation rules to ensure semantic models are:
- Syntactically correct (all templates resolved)
- Semantically valid (references exist, no duplicates)
- Logically consistent (no circular dependencies)
- Following best practices (descriptions, sample values)
"""

from typing import Any, Dict, List, Optional

from snowflake_semantic_tools.core.models import ValidationResult
from snowflake_semantic_tools.core.validation.rules import (
    DbtModelValidator,
    DependencyValidator,
    DuplicateValidator,
    QuotedTemplateValidator,
    ReferenceValidator,
    SemanticModelValidator,
    TemplateResolutionValidator,
)
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("semantic_validator")


class SemanticValidator:
    """
    Orchestrates multi-layer validation of semantic models.

    Implements a fail-fast validation strategy where critical errors
    stop further validation to avoid cascading false positives.

    Validation Order:
    1. **Quoted Templates** - Detects quoted template expressions (syntax check)
    2. **Template Resolution** - Must be resolved before other checks
    3. **dbt Models** - Validates the physical foundation
    4. **Semantic Models** - Validates semantic model structure
    5. **References** - Ensures all tables/columns exist
    6. **Duplicates** - Detects naming conflicts
    7. **Dependencies** - Identifies circular relationship chains

    Each validation layer can produce errors (blocking) or warnings
    (non-blocking), allowing flexible enforcement of standards while
    ensuring critical issues are addressed.
    """

    def __init__(self):
        """Initialize the validator with all rule checkers."""
        self.quoted_template_validator = QuotedTemplateValidator()
        self.dbt_model_validator = DbtModelValidator()
        self.semantic_model_validator = SemanticModelValidator()
        self.reference_validator = ReferenceValidator()
        self.duplicate_validator = DuplicateValidator()
        self.dependency_validator = DependencyValidator()
        self.template_validator = TemplateResolutionValidator()

    def validate(
        self,
        parse_result: Dict[str, Any],
        check_duplicates: bool = True,
        check_dependencies: bool = True,
        check_dbt_models: bool = True,
    ) -> ValidationResult:
        """
        Validate both dbt models and semantic models.

        Args:
            parse_result: Parsed data from Parser
            check_duplicates: Whether to check for duplicates
            check_dependencies: Whether to check dependencies
            check_dbt_models: Whether to validate dbt model requirements

        Returns:
            ValidationResult with all issues found
        """
        result = ValidationResult()

        # Extract dbt and semantic data
        dbt_data = parse_result.get("dbt", {})
        semantic_data = parse_result.get("semantic", {})

        # dbt Model validation (validate the source models first)
        if check_dbt_models and dbt_data:
            logger.debug("Validating dbt model definitions...")
            dbt_result = self.dbt_model_validator.validate(dbt_data)
            result.merge(dbt_result)

        # Quoted template validation (check syntax before template resolution)
        if semantic_data:
            logger.debug("Checking for quoted template expressions...")
            self.quoted_template_validator.validate(
                metrics_data=semantic_data.get("metrics", {}),
                relationships_data=semantic_data.get("relationships", {}),
                semantic_views_data=semantic_data.get("semantic_views", {}),
                filters_data=semantic_data.get("filters", {}),
                result=result,
            )

        # Template resolution check (must run after quoted template check)
        logger.debug("Checking template resolution...")
        template_errors, template_warnings = self.template_validator.validate(semantic_data)
        for error in template_errors:
            result.add_error(error, context={"type": "UNRESOLVED_TEMPLATE"})
        for warning in template_warnings:
            result.add_warning(warning, context={"type": "TEMPLATE_ISSUE"})

        # If templates are unresolved, other validations will fail
        if template_errors:
            logger.error(f"Found {len(template_errors)} unresolved templates. Fix these first.")
            return result

        # Semantic Model structure validation (validate structure before references)
        if semantic_data:
            logger.debug("Validating semantic model structure...")
            semantic_result = self.semantic_model_validator.validate(semantic_data)
            result.merge(semantic_result)

            # If semantic models have structural errors, continue but warn
            if semantic_result.error_count > 0:
                logger.warning(f"Found {semantic_result.error_count} semantic model structure errors")

        # Build catalogs from parsed data
        dbt_catalog = self._build_dbt_catalog(dbt_data)

        # Warn if dbt catalog is empty
        if not dbt_catalog:
            result.add_warning(
                "No dbt models found - table reference validation will be incomplete. "
                "Make sure dbt model files are available for full validation.",
                context={"type": "MISSING_DBT_CATALOG"},
            )
            logger.warning("dbt catalog is empty - table reference validation will be incomplete")

        # Reference validation (always run)
        logger.debug("Validating references...")
        ref_result = self.reference_validator.validate(semantic_data, dbt_catalog)
        result.merge(ref_result)

        # Duplicate detection
        if check_duplicates:
            logger.debug("Checking for duplicates...")
            dup_result = self.duplicate_validator.validate(semantic_data)
            result.merge(dup_result)

        # Dependency validation
        if check_dependencies:
            logger.debug("Validating dependencies...")
            dep_result = self.dependency_validator.validate(semantic_data)
            result.merge(dep_result)

        logger.debug(f"Validation complete: {result.error_count} errors, " f"{result.warning_count} warnings")

        return result

    def _build_dbt_catalog(self, dbt_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build a catalog of dbt models for reference validation.

        Args:
            dbt_data: Parsed dbt data

        Returns:
            Dictionary catalog of dbt models
        """
        catalog = {}

        # Use sm_tables which has the actual metadata (database, schema, etc.)
        tables = dbt_data.get("sm_tables", [])
        for table in tables:
            if isinstance(table, dict):
                # Table name from sm_tables is already uppercase, convert to lowercase for catalog key
                table_name = table.get("table_name", "").lower()
                if table_name:
                    # Store table with its metadata
                    catalog[table_name] = {
                        "name": table_name,
                        "database": table.get("database"),
                        "schema": table.get("schema"),
                        "primary_key": table.get("primary_key"),
                        "columns": {},
                    }

        # Add column information from dimensions, facts, and time_dimensions
        for col_type in ["sm_dimensions", "sm_facts", "sm_time_dimensions"]:
            columns = dbt_data.get(col_type, [])
            for col in columns:
                if isinstance(col, dict):
                    # Get the table this column belongs to
                    table_name = col.get("table_name", "").lower()
                    col_name = col.get("name", "").lower()

                    if table_name in catalog and col_name:
                        catalog[table_name]["columns"][col_name] = col

        # Also add models that might not be in sm_tables (for backward compatibility)
        models = dbt_data.get("models", [])
        for model in models:
            if isinstance(model, dict):
                model_name = model.get("name", "").lower()
                if model_name and model_name not in catalog:
                    # Only add if not already in catalog from sm_tables
                    catalog[model_name] = {
                        "name": model_name,
                        "database": model.get("database"),
                        "schema": model.get("schema"),
                        "columns": {},
                    }

                    # Add column information
                    for column in model.get("columns", []):
                        if isinstance(column, dict):
                            col_name = column.get("name", "").lower()
                            if col_name:
                                catalog[model_name]["columns"][col_name] = column

        return catalog
