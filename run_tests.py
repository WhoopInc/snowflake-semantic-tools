#!/usr/bin/env python3
"""
Comprehensive test runner for snowflake-semantic-tools.

This script provides various test execution options:
- Run all tests
- Run specific test categories (unit, integration, etc.)
- Run tests for specific modules
- Generate coverage reports
"""

import argparse
import sys
import subprocess
from pathlib import Path


def run_command(cmd, description):
    """Run a command and handle output."""
    print(f"\n{'=' * 60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print('=' * 60)
    
    result = subprocess.run(cmd, capture_output=False, text=True)
    
    if result.returncode != 0:
        print(f"\n[FAIL] {description} failed with exit code {result.returncode}")
        return False
    else:
        print(f"\n[PASS] {description} completed successfully")
        return True


def main():
    parser = argparse.ArgumentParser(description="Run tests for snowflake-semantic-tools")
    
    parser.add_argument(
        "--type",
        choices=["all", "unit", "integration", "snowflake", "smoke", "ci"],
        default="unit",
        help="Type of tests to run"
    )
    
    parser.add_argument(
        "--module",
        choices=[
            "all",
            "core_models",
            "parsing",
            "validation", 
            "generation",
            "infrastructure",
            "interfaces",
            "services",
            "shared"
        ],
        default="all",
        help="Specific module to test"
    )
    
    parser.add_argument(
        "--coverage",
        action="store_true",
        help="Generate coverage report"
    )
    
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output"
    )
    
    parser.add_argument(
        "--failfast",
        "-x",
        action="store_true",
        help="Stop on first failure"
    )
    
    parser.add_argument(
        "--parallel",
        "-n",
        type=int,
        help="Run tests in parallel with N workers"
    )
    
    parser.add_argument(
        "--html-report",
        action="store_true",
        help="Generate HTML test report"
    )
    
    args = parser.parse_args()
    
    # Build pytest command
    cmd = ["pytest"]
    
    # Add test selection
    if args.type == "all":
        cmd.append("tests/")
    elif args.type == "unit":
        cmd.append("tests/unit/")  # Run all unit tests without marker filtering
    elif args.type == "integration":
        cmd.append("tests/integration/")  # Run all integration tests
    elif args.type == "snowflake":
        cmd.extend(["-m", "snowflake"])
    elif args.type == "smoke":
        cmd.extend(["-m", "smoke"])
    elif args.type == "ci":
        cmd.extend(["-m", "ci"])
    
    # Add module selection
    if args.module != "all":
        module_map = {
            "core_models": "tests/unit/core/models/",
            "parsing": "tests/unit/core/parsing/",
            "validation": "tests/unit/core/validation/",
            "generation": "tests/unit/core/generation/",
            "infrastructure": "tests/unit/infrastructure/",
            "interfaces": "tests/unit/interfaces/",
            "services": "tests/unit/services/",
            "shared": "tests/unit/shared/"
        }
        if args.module in module_map:
            test_path = Path(module_map[args.module])
            if test_path.exists():
                cmd.append(str(test_path))
            else:
                print(f"Warning: Test path {test_path} not found, testing all")
    
    # Add options
    if not args.coverage:
        cmd.append("--no-cov")  # Disable coverage if not requested
    
    if args.verbose:
        cmd.append("-vv")
    
    if args.failfast:
        cmd.append("-x")
    
    if args.parallel:
        cmd.extend(["-n", str(args.parallel)])
    
    if args.html_report:
        cmd.extend(["--html=test_report.html", "--self-contained-html"])
    
    # Run the tests
    success = run_command(cmd, f"{args.type} tests")
    
    if not success:
        sys.exit(1)
    
    # Run additional checks if all tests pass
    if success and args.type in ["all", "ci"]:
        print("\n" + "=" * 60)
        print("Running additional checks...")
        print("=" * 60)
        
        # Run linting
        lint_cmd = ["python", "-m", "flake8", "snowflake_semantic_tools", "--max-line-length=120"]
        run_command(lint_cmd, "Linting check")
        
        # Run type checking (if mypy is installed)
        try:
            type_cmd = ["python", "-m", "mypy", "snowflake_semantic_tools", "--ignore-missing-imports"]
            run_command(type_cmd, "Type checking")
        except:
            print("Skipping type checking (mypy not installed)")
    
    # Print coverage summary if coverage was generated
    if args.coverage and success:
        print("\n" + "=" * 60)
        print("Coverage Summary")
        print("=" * 60)
        print("HTML coverage report generated at: htmlcov/index.html")
        print("XML coverage report generated at: coverage.xml")
    
    print("\n" + "=" * 60)
    print("All tests completed successfully!" if success else "Some tests failed")
    print("=" * 60)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
