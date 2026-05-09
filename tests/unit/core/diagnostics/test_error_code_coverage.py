"""
Error Code Coverage Test

Verifies that every registered error code in the registry is actually used
in at least one validation rule. Dead/orphaned codes are detected and flagged.

Also verifies that every code used in rules IS registered (no typos/unregistered codes).
"""

import re
from pathlib import Path

import pytest

from snowflake_semantic_tools.core.diagnostics import ERRORS


RULES_DIR = Path(__file__).parents[4] / "snowflake_semantic_tools" / "core" / "validation" / "rules"
SERVICES_DIR = Path(__file__).parents[4] / "snowflake_semantic_tools" / "services"


def _find_all_used_codes() -> set:
    """Scan all validation rules and services for rule_id= assignments."""
    used_codes = set()
    search_dirs = [RULES_DIR, SERVICES_DIR]

    for search_dir in search_dirs:
        for f in search_dir.glob("*.py"):
            content = f.read_text()
            for match in re.finditer(r'rule_id="(SST-[A-Z]\d{3})"', content):
                used_codes.add(match.group(1))

    return used_codes


class TestErrorCodeCoverage:
    """Ensure every registered code is used and every used code is registered."""

    def test_all_registered_codes_are_used(self):
        """Every validation code (SST-V*) in error_codes.py must appear in at least one rule_id= assignment.
        
        Note: SST-P/E/G/C codes are for parsing, extract, generate, and config error paths
        which are handled outside the validation rules. Those are excluded from this check.
        """
        used_codes = _find_all_used_codes()
        validation_codes = {k for k in ERRORS.keys() if k.startswith("SST-V")}

        unused = validation_codes - used_codes
        assert unused == set(), (
            f"Found {len(unused)} registered SST-V codes that are NEVER used in any validation rule.\n"
            f"Either use them or remove from error_codes.py:\n\n"
            + "\n".join(f"  {code}: {ERRORS[code].title}" for code in sorted(unused))
        )

    def test_all_used_codes_are_registered(self):
        """Every rule_id= value in the codebase must exist in the registry."""
        used_codes = _find_all_used_codes()
        registered_codes = set(ERRORS.keys())

        unregistered = used_codes - registered_codes
        assert unregistered == set(), (
            f"Found {len(unregistered)} rule_id values NOT in the error code registry.\n"
            f"Add them to core/diagnostics/error_codes.py:\n\n"
            + "\n".join(f"  {code}" for code in sorted(unregistered))
        )

    def test_no_duplicate_codes_in_registry(self):
        """No duplicate codes in the registry (sanity check)."""
        from snowflake_semantic_tools.core.diagnostics.error_codes import ERRORS as registry

        codes = list(registry.keys())
        assert len(codes) == len(set(codes)), "Duplicate codes found in registry"

    def test_code_format_consistency(self):
        """All codes follow SST-{LETTER}{3DIGITS} format."""
        for code in ERRORS:
            assert re.match(r"^SST-[VPEGC]\d{3}$", code), (
                f"Code '{code}' doesn't match expected format SST-{{V|P|E|G|C}}XXX"
            )

    def test_all_codes_have_nonempty_title(self):
        """Every code must have a meaningful title."""
        for code, spec in ERRORS.items():
            assert spec.title and len(spec.title) >= 5, (
                f"Code {code} has missing or too-short title: '{spec.title}'"
            )

    def test_all_codes_have_suggestion_template(self):
        """Every code must have a suggestion template."""
        for code, spec in ERRORS.items():
            assert spec.suggestion_template, (
                f"Code {code} ({spec.title}) has no suggestion_template"
            )
