#!/usr/bin/env python3
"""
Test Coverage Analysis Tool

Analyzes current test coverage and identifies areas needing more tests.
"""

import ast
import sys
from pathlib import Path
from typing import Dict, List, Set
import importlib.util


def analyze_source_files(source_dir: Path) -> Dict[str, Dict]:
    """
    Analyze source files to identify classes, functions, and methods.
    
    Args:
        source_dir: Path to source directory
        
    Returns:
        Dictionary mapping file paths to their contents
    """
    source_analysis = {}
    
    for py_file in source_dir.rglob("*.py"):
        if "__pycache__" in str(py_file) or py_file.name == "__init__.py":
            continue
            
        try:
            with open(py_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            
            classes = []
            functions = []
            methods = []
            
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    classes.append(node.name)
                    
                    # Get methods in this class
                    class_methods = []
                    for item in node.body:
                        if isinstance(item, ast.FunctionDef):
                            class_methods.append(item.name)
                    methods.extend([(node.name, method) for method in class_methods])
                    
                elif isinstance(node, ast.FunctionDef):
                    # Top-level functions
                    if not any(isinstance(parent, ast.ClassDef) for parent in ast.walk(tree) 
                             if hasattr(parent, 'body') and node in getattr(parent, 'body', [])):
                        functions.append(node.name)
            
            relative_path = py_file.relative_to(source_dir)
            source_analysis[str(relative_path)] = {
                "classes": classes,
                "functions": functions,
                "methods": methods,
                "line_count": len(content.splitlines())
            }
            
        except Exception as e:
            print(f"Error analyzing {py_file}: {e}")
    
    return source_analysis


def analyze_test_files(test_dir: Path) -> Dict[str, Dict]:
    """
    Analyze test files to identify what's being tested.
    
    Args:
        test_dir: Path to test directory
        
    Returns:
        Dictionary mapping test files to their test coverage
    """
    test_analysis = {}
    
    for py_file in test_dir.rglob("test_*.py"):
        try:
            with open(py_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            
            test_classes = []
            test_methods = []
            
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef) and node.name.startswith("Test"):
                    test_classes.append(node.name)
                    
                    # Get test methods in this class
                    for item in node.body:
                        if isinstance(item, ast.FunctionDef) and item.name.startswith("test_"):
                            test_methods.append((node.name, item.name))
                
                elif isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
                    test_methods.append(("", node.name))
            
            relative_path = py_file.relative_to(test_dir)
            test_analysis[str(relative_path)] = {
                "test_classes": test_classes,
                "test_methods": test_methods,
                "line_count": len(content.splitlines())
            }
            
        except Exception as e:
            print(f"Error analyzing test {py_file}: {e}")
    
    return test_analysis


def identify_coverage_gaps(source_analysis: Dict, test_analysis: Dict) -> Dict:
    """
    Identify gaps in test coverage.
    
    Args:
        source_analysis: Analysis of source files
        test_analysis: Analysis of test files
        
    Returns:
        Dictionary of coverage gaps and recommendations
    """
    gaps = {
        "untested_classes": [],
        "untested_functions": [],
        "low_coverage_files": [],
        "missing_test_files": [],
        "recommendations": []
    }
    
    # Find source files without corresponding test files
    for source_file in source_analysis:
        # Convert source path to expected test path
        expected_test_file = f"test_{source_file}"
        
        if expected_test_file not in test_analysis:
            gaps["missing_test_files"].append(source_file)
    
    # Find untested classes and functions
    for source_file, source_data in source_analysis.items():
        corresponding_test = f"test_{source_file}"
        
        if corresponding_test in test_analysis:
            test_data = test_analysis[corresponding_test]
            tested_classes = {tc.replace("Test", "") for tc in test_data["test_classes"]}
            
            # Find untested classes
            for class_name in source_data["classes"]:
                if class_name not in tested_classes:
                    gaps["untested_classes"].append(f"{source_file}::{class_name}")
            
            # Find untested functions
            tested_functions = {tm[1].replace("test_", "") for tm in test_data["test_methods"]}
            for function_name in source_data["functions"]:
                if function_name not in tested_functions:
                    gaps["untested_functions"].append(f"{source_file}::{function_name}")
        
        # Identify files with low coverage (high LOC but few tests)
        if source_data["line_count"] > 100:
            test_file = f"test_{source_file}"
            if test_file in test_analysis:
                test_count = len(test_analysis[test_file]["test_methods"])
                if test_count < 5:  # Arbitrary threshold
                    gaps["low_coverage_files"].append({
                        "file": source_file,
                        "lines": source_data["line_count"],
                        "tests": test_count
                    })
    
    # Generate recommendations
    if gaps["missing_test_files"]:
        gaps["recommendations"].append(
            f"Create test files for {len(gaps['missing_test_files'])} source files without tests"
        )
    
    if gaps["untested_classes"]:
        gaps["recommendations"].append(
            f"Add tests for {len(gaps['untested_classes'])} untested classes"
        )
    
    if gaps["low_coverage_files"]:
        gaps["recommendations"].append(
            f"Increase test coverage for {len(gaps['low_coverage_files'])} files with low test density"
        )
    
    return gaps


def main():
    """Run coverage analysis."""
    project_root = Path(__file__).parent.parent
    source_dir = project_root / "snowflake_semantic_tools"
    test_dir = project_root / "tests" / "unit"
    
    print("🔍 Analyzing Test Coverage for Snowflake Semantic Tools")
    print("=" * 60)
    
    # Analyze source and test files
    print("📁 Analyzing source files...")
    source_analysis = analyze_source_files(source_dir)
    
    print("🧪 Analyzing test files...")
    test_analysis = analyze_test_files(test_dir)
    
    print("🎯 Identifying coverage gaps...")
    gaps = identify_coverage_gaps(source_analysis, test_analysis)
    
    # Print summary
    print(f"\\n📊 Coverage Summary:")
    print(f"Source files analyzed: {len(source_analysis)}")
    print(f"Test files found: {len(test_analysis)}")
    print(f"Missing test files: {len(gaps['missing_test_files'])}")
    print(f"Untested classes: {len(gaps['untested_classes'])}")
    print(f"Untested functions: {len(gaps['untested_functions'])}")
    print(f"Low coverage files: {len(gaps['low_coverage_files'])}")
    
    # Print detailed gaps
    if gaps["missing_test_files"]:
        print(f"\\n❌ Missing Test Files:")
        for file in gaps["missing_test_files"][:10]:  # Show first 10
            print(f"  - {file}")
        if len(gaps["missing_test_files"]) > 10:
            print(f"  ... and {len(gaps['missing_test_files']) - 10} more")
    
    if gaps["untested_classes"]:
        print(f"\\n⚠️  Untested Classes:")
        for class_ref in gaps["untested_classes"][:10]:
            print(f"  - {class_ref}")
        if len(gaps["untested_classes"]) > 10:
            print(f"  ... and {len(gaps['untested_classes']) - 10} more")
    
    if gaps["low_coverage_files"]:
        print(f"\\n📈 Files Needing More Tests:")
        for file_info in gaps["low_coverage_files"]:
            print(f"  - {file_info['file']} ({file_info['lines']} lines, {file_info['tests']} tests)")
    
    # Print recommendations
    if gaps["recommendations"]:
        print(f"\\n💡 Recommendations:")
        for rec in gaps["recommendations"]:
            print(f"  - {rec}")
    
    # Calculate rough coverage percentage
    total_classes = sum(len(data["classes"]) for data in source_analysis.values())
    tested_classes = len([tc for tc in gaps["untested_classes"]])
    coverage_pct = ((total_classes - tested_classes) / total_classes * 100) if total_classes > 0 else 0
    
    print(f"\\n🎯 Estimated Class Coverage: {coverage_pct:.1f}%")
    
    if coverage_pct < 80:
        print("🔴 Coverage below 80% - consider adding more tests")
    elif coverage_pct < 90:
        print("🟡 Coverage good but could be improved")
    else:
        print("🟢 Excellent test coverage!")


if __name__ == "__main__":
    main()
