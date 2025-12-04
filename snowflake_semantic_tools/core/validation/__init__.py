"""
Validation Module

Multi-layer validation system ensuring semantic model integrity and correctness.

## Validation Strategy

The module implements a hierarchical validation approach:

1. **Template Resolution** - Ensures all {{ }} references are resolved
2. **dbt Model Validation** - Validates physical layer foundation
3. **Semantic Model Validation** - Validates semantic model structure
4. **Reference Validation** - Confirms all table/column references exist
5. **Duplicate Validation** - Prevents naming conflicts
6. **Dependency Validation** - Detects circular dependencies in relationships

## Validation Rules

- **DbtModelValidator**: Enforces dbt model requirements and best practices
- **SemanticModelValidator**: Validates semantic model structure and required fields
- **ReferenceValidator**: Validates table/column references against dbt catalog
- **DuplicateValidator**: Identifies duplicate metric/relationship/filter names
- **DependencyValidator**: Ensures no circular dependencies in relationships
- **TemplateResolutionValidator**: Confirms all templates are properly resolved
- **QuotedTemplateValidator**: Detects quoted template expressions that cause Snowflake errors

## Error Severity

Validations produce three severity levels:
- **ERROR**: Critical issues that prevent semantic model generation
- **WARNING**: Issues that should be reviewed but don't block processing
- **INFO**: Suggestions for improving model quality

The validation order is critical - template resolution must occur first,
as unresolved templates make other validations unreliable.
"""

from snowflake_semantic_tools.core.validation.rules import (
    DbtModelValidator,
    DependencyValidator,
    DuplicateValidator,
    QuotedTemplateValidator,
    ReferenceValidator,
    SemanticModelValidator,
    TemplateResolutionValidator,
)
from snowflake_semantic_tools.core.validation.validator import SemanticValidator

__all__ = [
    "SemanticValidator",
    "DuplicateValidator",
    "DependencyValidator",
    "ReferenceValidator",
    "TemplateResolutionValidator",
    "DbtModelValidator",
    "SemanticModelValidator",
    "QuotedTemplateValidator",
]
