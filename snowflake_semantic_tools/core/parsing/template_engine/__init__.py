"""
Template Engine for Snowflake Semantic Tools

Handles resolution of template references in YAML content:
- {{ table('name') }} - Table references
- {{ column('table', 'column') }} - Column references
- {{ metric('name') }} - Metric composition
- {{ custom_instructions('name') }} - Custom instruction references
"""

from snowflake_semantic_tools.core.parsing.template_engine.resolver import TemplateResolver
from snowflake_semantic_tools.core.parsing.template_engine.validators import HardcodedValueDetector

__all__ = ["TemplateResolver", "HardcodedValueDetector"]
