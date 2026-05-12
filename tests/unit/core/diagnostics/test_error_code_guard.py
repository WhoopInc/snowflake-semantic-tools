"""
CI Guard: Ensures all add_error/add_warning/add_info calls have rule_id=.

This test fails if ANY call site in the validation rules is missing a rule_id,
preventing regressions where new errors are added without proper error codes.
"""

import re
from pathlib import Path

import pytest


RULES_DIR = Path(__file__).parents[4] / "snowflake_semantic_tools" / "core" / "validation" / "rules"


class TestErrorCodeGuard:
    """Prevent any add_error/add_warning without rule_id from being committed."""

    def test_all_calls_have_rule_id(self):
        """Every result.add_error/add_warning/add_info call must include rule_id=."""
        uncoded = []

        for f in sorted(RULES_DIR.glob("*.py")):
            if f.name == "__init__.py":
                continue
            lines = f.read_text().splitlines()

            for i, line in enumerate(lines, 1):
                if re.search(r"result\.add_(error|warning|info)\(", line):
                    block = "\n".join(lines[i - 1 : min(i + 20, len(lines))])
                    if "rule_id=" not in block:
                        uncoded.append(f"{f.name}:{i}")

        assert uncoded == [], (
            f"Found {len(uncoded)} add_error/add_warning/add_info calls WITHOUT rule_id=.\n"
            f"Every error must have an error code for diagnostic reporting.\n\n"
            f"Uncoded locations:\n" + "\n".join(f"  {loc}" for loc in uncoded)
        )

    def test_all_calls_have_suggestion(self):
        """Every result.add_error/add_warning call must include suggestion=."""
        missing_suggestion = []

        for f in sorted(RULES_DIR.glob("*.py")):
            if f.name == "__init__.py":
                continue
            lines = f.read_text().splitlines()

            for i, line in enumerate(lines, 1):
                if re.search(r"result\.add_(error|warning)\(", line):
                    block = "\n".join(lines[i - 1 : min(i + 20, len(lines))])
                    if "suggestion=" not in block:
                        missing_suggestion.append(f"{f.name}:{i}")

        assert missing_suggestion == [], (
            f"Found {len(missing_suggestion)} add_error/add_warning calls WITHOUT suggestion=.\n"
            f"Every error must have an actionable suggestion for the user.\n\n"
            f"Missing locations:\n" + "\n".join(f"  {loc}" for loc in missing_suggestion)
        )

    def test_no_unknown_error_codes(self):
        """All rule_id values used in code must exist in the error code registry."""
        from snowflake_semantic_tools.core.diagnostics import ERRORS

        used_codes = set()

        for f in sorted(RULES_DIR.glob("*.py")):
            if f.name == "__init__.py":
                continue
            content = f.read_text()
            for match in re.finditer(r'rule_id="(SST-[A-Z]\d{3})"', content):
                used_codes.add(match.group(1))

        unknown = used_codes - set(ERRORS.keys())
        assert unknown == set(), (
            f"Found rule_id values not registered in error_codes.py: {sorted(unknown)}\n"
            f"Add them to snowflake_semantic_tools/core/diagnostics/error_codes.py"
        )
