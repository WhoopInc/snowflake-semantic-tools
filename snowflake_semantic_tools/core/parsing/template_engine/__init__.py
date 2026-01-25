"""
Template Engine for Snowflake Semantic Tools

Handles resolution of template references in YAML content:
- {{ ref('name') }} - Unified syntax for table references (recommended)
- {{ ref('table', 'column') }} - Unified syntax for column references (recommended)
- {{ table('name') }} - Legacy table references (still supported)
- {{ column('table', 'column') }} - Legacy column references (still supported)
- {{ metric('name') }} - Metric composition
- {{ custom_instructions('name') }} - Custom instruction references
"""

from snowflake_semantic_tools.core.parsing.template_engine.resolver import TemplateResolver
from snowflake_semantic_tools.core.parsing.template_engine.validators import HardcodedValueDetector

__all__ = ["TemplateResolver", "HardcodedValueDetector"]
