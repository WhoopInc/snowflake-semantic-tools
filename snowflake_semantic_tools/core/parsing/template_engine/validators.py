"""
Template Validators

Validators for detecting issues with template usage in semantic models.
"""

import re
from typing import List, Optional, Set


class HardcodedValueDetector:
    """
    Detects hardcoded values that should use template syntax.

    This validator helps maintain consistency by identifying places where
    template syntax should be used instead of hardcoded values.
    """

    def __init__(self, dbt_tables: Optional[Set[str]] = None):
        """
        Initialize the detector with known dbt tables.

        Args:
            dbt_tables: Set of known dbt table names for validation
        """
        self.dbt_tables = dbt_tables or set()
        self.warnings = []

    def check_for_hardcoded_values(self, content: str, file_path: str) -> List[str]:
        """
        Check YAML content for hardcoded values that should use templates.

        Scans the content for:
        - Hardcoded table references that should use {{ table() }}
        - Hardcoded column references that should use {{ column() }}
        - Other values that would benefit from template syntax

        Args:
            content: YAML content to check
            file_path: Path to the file being checked (for error messages)

        Returns:
            List of warning messages for found issues
        """
        warnings = []
        lines = content.split("\n")
        in_synonyms_section = False
        in_sample_values_section = False

        for line_num, line in enumerate(lines, 1):
            # Skip comments and empty lines
            if line.strip().startswith("#") or not line.strip():
                continue

            # Track sections where hardcoded values are expected
            if "synonyms:" in line:
                in_synonyms_section = True
                continue
            elif "sample_values:" in line:
                in_sample_values_section = True
                continue
            elif line.strip() and not line[0].isspace() and ":" in line:
                # New top-level key, reset section flags
                in_synonyms_section = False
                in_sample_values_section = False

            # Skip validation in sections where hardcoded values are normal
            if in_synonyms_section or in_sample_values_section:
                continue

            # Check for hardcoded table references
            warnings.extend(self._check_table_references(lines, line_num, line, file_path))

            # Check for hardcoded column references in expressions
            warnings.extend(self._check_column_references(lines, line_num, line, file_path))

        return warnings

    def _check_table_references(self, lines: List[str], line_num: int, line: str, file_path: str) -> List[str]:
        """Check for hardcoded table references in 'tables:' sections."""
        warnings = []

        if "tables:" in line:
            # Look at the next few lines for table entries
            for i in range(1, min(10, len(lines) - line_num)):
                next_line = lines[line_num + i - 1]

                # Stop if we hit another section
                if ":" in next_line and not next_line.strip().startswith("-"):
                    break

                if next_line.strip().startswith("-"):
                    table_entry = next_line.strip().lstrip("- ").strip()

                    # Check if it's not using {{ table() }} syntax
                    if (
                        table_entry
                        and "{{" not in table_entry
                        and not table_entry.startswith("#")
                        and not table_entry.startswith("name:")
                    ):
                        # Check if this looks like a table name
                        if "_" in table_entry or table_entry.lower() in [t.lower() for t in self.dbt_tables]:
                            warnings.append(
                                f"WARNING: {file_path}:{line_num + i}: "
                                f"Hardcoded table reference '{table_entry}' "
                                f"should use template syntax: {{{{ table('{table_entry}') }}}}"
                            )

        return warnings

    def _check_column_references(self, lines: List[str], line_num: int, line: str, file_path: str) -> List[str]:
        """Check for hardcoded column references in expressions."""
        warnings = []

        if "expr:" in line or line.strip().startswith("expr:"):
            expr_start = line_num
            expr_lines = []

            # Collect multi-line expressions
            if "|" in line or ">" in line:  # Multi-line expression indicators
                for i in range(1, min(20, len(lines) - line_num)):
                    next_line = lines[line_num + i - 1]
                    if next_line and not next_line[0].isspace() and ":" in next_line:
                        break
                    expr_lines.append(next_line)
            else:
                # Single line expression
                expr_lines = [line.split("expr:")[1] if "expr:" in line else line]

            expr_content = " ".join(expr_lines).strip()

            # Look for TABLE.COLUMN patterns not using {{ column() }}
            if expr_content and not expr_content.startswith("#"):
                # Pattern for TABLE.COLUMN references
                pattern = r"\b([A-Z_]+)\.([A-Z_]+)\b"
                matches = re.findall(pattern, expr_content.upper())

                for table, column in matches:
                    template_form = f"{{{{ column('{table.lower()}', '{column.lower()}') }}}}"
                    if template_form not in expr_content:
                        # Check if this is likely a real table reference
                        if table.lower() in [t.lower() for t in self.dbt_tables] or "_" in table:
                            warnings.append(
                                f"WARNING: {file_path}:{expr_start}: "
                                f"Hardcoded column reference '{table}.{column}' "
                                f"should use template syntax: {template_form}"
                            )

        return warnings
