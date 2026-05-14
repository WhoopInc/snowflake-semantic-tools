# Test Suite for Snowflake Semantic Tools

Comprehensive test suite with organized fixtures and complete coverage.

## Structure

```
tests/
├── fixtures/                    # Test data organized by validation severity
│   ├── errors/                 # ValidationError scenarios
│   ├── warnings/               # ValidationWarning scenarios
│   ├── info/                   # ValidationInfo scenarios
│   ├── success/                # ValidationSuccess scenarios
│   ├── sample_project/         # Complete dbt+SST project for integration tests
│   ├── sample_project_factory.py  # Factory for creating test project variants
│   └── __init__.py            # Fixture loading utilities
├── unit/                       # Unit tests mirroring source structure
│   ├── core/
│   │   ├── models/            # Data model tests
│   │   ├── parsing/           # Parser tests
│   │   │   ├── parsers/       # Specialized parser tests
│   │   │   └── template_engine/ # Template resolution tests
│   │   ├── validation/        # Validation tests
│   │   │   └── rules/         # Individual validation rule tests
│   │   └── generation/        # Generation tests
│   ├── infrastructure/
│   │   ├── git/              # Git integration tests
│   │   └── snowflake/        # Snowflake integration tests
│   ├── interfaces/
│   │   ├── api/              # Python API tests
│   │   └── cli/              # CLI tests
│   │       └── commands/     # Individual command tests
│   ├── services/             # Service orchestration tests
│   └── shared/               # Shared utility tests
├── integration/                # Integration tests
└── performance/               # Performance benchmarks
```

## Fixture Organization

### By Severity
- **errors/**: Scenarios that should produce `ValidationError`
- **warnings/**: Scenarios that should produce `ValidationWarning`
- **info/**: Scenarios that should produce `ValidationInfo`
- **success/**: Scenarios that should pass validation cleanly

### By Component
Each severity level contains fixtures for:
- **metrics/**: Metric validation scenarios
- **relationships/**: Relationship validation scenarios
- **filters/**: Filter validation scenarios
- **custom_instructions/**: Custom instruction scenarios
- **verified_queries/**: Verified query scenarios
- **semantic_views/**: Semantic view scenarios
- **dbt_models/**: dbt model validation scenarios
- **templates/**: Template resolution scenarios

## Sample Project

A minimal but complete dbt+SST project is embedded in `tests/fixtures/sample_project/`
for integration testing. This provides a realistic, self-contained test harness.

### Sample Project Structure

```
sample_project/
├── dbt_project.yml              # dbt project configuration
├── sst_config.yaml              # SST configuration
├── profiles.yml.template        # Template for Snowflake credentials
├── models/
│   ├── staging/                 # Staging models (stg_customers, stg_orders, stg_products)
│   └── marts/                   # Mart models with SST metadata (customers, orders, products)
├── target/
│   └── manifest.json            # Pre-generated dbt manifest
└── snowflake_semantic_models/
    ├── semantic_views.yml       # Semantic view definitions
    ├── metrics/                 # Metric definitions
    ├── relationships/           # Relationship definitions
    ├── filters/                 # Filter definitions
    ├── verified_queries/        # Verified query examples
    └── custom_instructions/     # Custom instruction examples
```

### Using the Sample Project

```python
from tests.fixtures.sample_project_factory import SampleProjectFactory

# Create a clean copy for testing
project = SampleProjectFactory.create(tmp_path)

# Create variants for specific test scenarios
project = SampleProjectFactory.create_without_sst(tmp_path)      # For init tests
project = SampleProjectFactory.create_unenriched(tmp_path)       # For enrich tests
project = SampleProjectFactory.create_with_invalid_metrics(tmp_path)  # For validation error tests
project = SampleProjectFactory.create_without_manifest(tmp_path)  # For compile-required tests
project = SampleProjectFactory.create_with_profiles(tmp_path)    # With profiles.yml
```

### Integration Test Base Class

```python
from tests.integration.base import IntegrationTestBase

class TestMyFeature(IntegrationTestBase):
    def test_with_sample_project(self, sample_project):
        """Use the sample_project fixture for a clean copy."""
        assert (sample_project / "dbt_project.yml").exists()

    def test_in_project_context(self, project_context):
        """Run tests with working directory set to project."""
        # Current working directory is the sample project
        import os
        assert "dbt_project.yml" in os.listdir()
```

### Sample Project Contents

**Models (3 tables):**
- `customers` - Customer dimension with aggregated order metrics
- `orders` - Order fact table with transaction details
- `products` - Product dimension with catalog information

**Metrics (5):**
- `total_orders`, `total_revenue`, `total_customers`, `average_order_value`, `total_products_sold`

**Relationships (2):**
- `orders_to_customers`, `orders_to_products`

**Semantic View (1):**
- `sample_analytics` - Combines all 3 tables

## Usage Examples

### Loading Fixtures

```python
from tests.fixtures import load_fixture, get_fixtures_by_severity

# Load specific fixture
data = load_fixture('errors/metrics/circular_dependency.yml')

# Get all error fixtures
error_fixtures = get_fixtures_by_severity('errors')

# Get all metric fixtures across severities
metric_fixtures = get_fixtures_by_component('metrics')
```

### Writing Tests with Fixtures

```python
import pytest
from tests.fixtures import get_fixtures_by_component

class TestMetricValidation:
    @pytest.mark.parametrize("fixture_path", get_fixtures_by_component('metrics'))
    def test_metric_scenarios(self, fixture_path, validator):
        fixture_data = load_fixture(str(fixture_path.relative_to(FIXTURES_DIR)))
        result = validator.validate(fixture_data)
        
        if 'errors' in str(fixture_path):
            assert not result.is_valid
        elif 'success' in str(fixture_path):
            assert result.is_valid
```

## Test Categories

### Unit Tests
- Test individual functions and classes in isolation
- Mock external dependencies
- Fast execution (< 1 second per test)
- High coverage of edge cases

### Integration Tests
- Test component interactions
- Use real (but minimal) external resources
- Moderate execution time (1-10 seconds per test)
- Focus on interface contracts

### Performance Tests
- Benchmark critical operations
- Test with large datasets
- Identify performance regressions
- Longer execution time acceptable

## Running Tests

### All Tests
```bash
pytest tests/
```

### By Category
```bash
# Unit tests only
pytest tests/unit/

# Integration tests only
pytest tests/integration/

# Performance tests only
pytest tests/performance/
```

### By Component
```bash
# Core validation tests
pytest tests/unit/core/validation/

# CLI tests
pytest tests/unit/interfaces/cli/

# Snowflake integration tests
pytest tests/integration/infrastructure/snowflake/
```

### With Coverage
```bash
# Generate coverage report
pytest --cov=snowflake_semantic_tools --cov-report=html tests/unit/

# View coverage
open htmlcov/index.html
```

### Specific Fixture Testing
```bash
# Test all error scenarios
pytest tests/unit/core/validation/test_validation_comprehensive.py::TestValidationScenarios::test_error_scenarios

# Test specific component
pytest tests/unit/core/validation/test_validation_comprehensive.py::TestValidationScenarios::test_metrics_validation
```

## Test Data Management

### Fixture Best Practices
1. **Realistic Data**: Use realistic table/column names and structures
2. **Focused Scenarios**: Each fixture tests one specific validation rule
3. **Clear Naming**: Fixture names clearly indicate what they test
4. **Complete Coverage**: Cover all validation paths (error, warning, success)

### Adding New Fixtures
1. Identify the validation scenario to test
2. Choose appropriate severity level (`errors/`, `warnings/`, `success/`)
3. Choose appropriate component type (`metrics/`, `relationships/`, etc.)
4. Create YAML file with descriptive name
5. Add test case to use the new fixture

### Fixture Validation
```bash
# Ensure all fixtures are valid YAML
pytest tests/unit/core/validation/test_validation_comprehensive.py::TestFixtureCompleteness::test_fixtures_are_valid_yaml

# Ensure fixture completeness
pytest tests/unit/core/validation/test_validation_comprehensive.py::TestFixtureCompleteness
```

## Continuous Integration

The test suite is designed for CI/CD integration:

### Fast Feedback
- Unit tests run in < 30 seconds
- Immediate feedback on code changes
- Comprehensive coverage of business logic

### Integration Validation
- Integration tests validate external interfaces
- Run on merge to main branch
- Catch integration regressions

### Performance Monitoring
- Performance tests run nightly
- Track performance trends over time
- Alert on significant regressions

## Contributing

When adding new features:

1. **Add Unit Tests**: Test new functions/classes in isolation
2. **Add Integration Tests**: Test component interactions
3. **Add Fixtures**: Create test data for new validation scenarios
4. **Update Documentation**: Keep this README current

When fixing bugs:

1. **Add Regression Test**: Reproduce the bug with a test
2. **Fix the Code**: Implement the fix
3. **Verify Fix**: Ensure the test now passes
4. **Add Fixture**: Add fixture to prevent regression

## Test Quality Metrics

- **Coverage Target**: > 90% line coverage
- **Test Speed**: Unit tests < 30s total
- **Test Reliability**: < 1% flaky test rate
- **Fixture Coverage**: All validation rules have error/success fixtures
