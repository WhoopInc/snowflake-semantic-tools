"""
Test Fixtures for Snowflake Semantic Tools

Comprehensive test fixtures organized by validation severity and component type.

## Structure

```
fixtures/
├── errors/          # ValidationError scenarios
├── warnings/        # ValidationWarning scenarios  
├── info/           # ValidationInfo scenarios
├── success/        # ValidationSuccess scenarios
└── edge_cases/     # Edge cases and stress tests
```

## Component Types

Each severity level contains fixtures for:
- metrics/           # Metric validation scenarios
- relationships/     # Relationship validation scenarios
- filters/          # Filter validation scenarios
- custom_instructions/ # Custom instruction scenarios
- verified_queries/  # Verified query scenarios
- semantic_views/    # Semantic view scenarios
- dbt_models/       # dbt model validation scenarios
- templates/        # Template resolution scenarios

## Usage

```python
from tests.fixtures import load_fixture, get_fixtures_by_severity

# Load specific fixture
metric_data = load_fixture('errors/metrics/circular_dependency.yml')

# Get all fixtures for a severity level
error_fixtures = get_fixtures_by_severity('errors')
```
"""

from pathlib import Path
from typing import Any, Dict, List

import yaml

FIXTURES_DIR = Path(__file__).parent


def load_fixture(fixture_path: str) -> Dict[str, Any]:
    """
    Load a YAML fixture file.

    Args:
        fixture_path: Relative path from fixtures directory

    Returns:
        Parsed YAML content
    """
    full_path = FIXTURES_DIR / fixture_path
    with open(full_path, "r") as f:
        return yaml.safe_load(f)


def get_fixtures_by_severity(severity: str) -> List[Path]:
    """
    Get all fixture files for a given severity level.

    Args:
        severity: One of 'errors', 'warnings', 'info', 'success'

    Returns:
        List of fixture file paths
    """
    severity_dir = FIXTURES_DIR / severity
    if not severity_dir.exists():
        return []

    return list(severity_dir.rglob("*.yml"))


def get_fixtures_by_component(component: str) -> List[Path]:
    """
    Get all fixture files for a given component type.

    Args:
        component: Component type (e.g., 'metrics', 'relationships')

    Returns:
        List of fixture file paths
    """
    fixtures = []
    for severity_dir in FIXTURES_DIR.iterdir():
        if severity_dir.is_dir() and not severity_dir.name.startswith("."):
            component_dir = severity_dir / component
            if component_dir.exists():
                fixtures.extend(component_dir.glob("*.yml"))

    return fixtures


def get_all_fixtures() -> Dict[str, List[Path]]:
    """
    Get all fixtures organized by severity.

    Returns:
        Dictionary mapping severity to list of fixture paths
    """
    fixtures = {}
    for severity in ["errors", "warnings", "info", "success"]:
        fixtures[severity] = get_fixtures_by_severity(severity)

    return fixtures


# Fixture categories for easy reference
VALIDATION_SEVERITIES = ["errors", "warnings", "info", "success"]
COMPONENT_TYPES = [
    "metrics",
    "relationships",
    "filters",
    "custom_instructions",
    "verified_queries",
    "semantic_views",
    "dbt_models",
    "templates",
]
