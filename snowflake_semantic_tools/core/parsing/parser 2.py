"""
Main Parser

Orchestrates the parsing pipeline for transforming YAML definitions into Snowflake semantic models.

This parser bridges the gap between dbt's physical data layer and Snowflake's semantic layer
by extracting, validating, and transforming metadata from both dbt model definitions and
semantic model YAML files.
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from snowflake_semantic_tools.core.parsing.file_detector import FileTypeDetector
from snowflake_semantic_tools.core.parsing.parsers import ErrorTracker, dbt_parser, semantic_parser
from snowflake_semantic_tools.core.parsing.template_engine import HardcodedValueDetector, TemplateResolver
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("parser")


class ParsingCriticalError(Exception):
    """
    Raised when critical parsing errors occur that should prevent further validation.

    This exception is raised when template resolution fails or other critical
    parsing issues occur that would make validation unreliable or misleading.
    """

    def __init__(self, message: str, errors: List[str]):
        super().__init__(message)
        self.errors = errors


class Parser:
    """
    Central orchestrator for parsing YAML files into Snowflake semantic models.

    The Parser implements a two-pass strategy to handle complex dependencies:

    **Pass 1 - Catalog Building**: Scans all files to build reference catalogs
    - dbt models → physical table catalog
    - Metrics → reusable metric definitions
    - Custom instructions → AI guidance rules

    **Pass 2 - Template Resolution**: Resolves all template references
    - Expands {{ table() }} to actual table names
    - Resolves {{ column() }} to fully qualified references
    - Composes {{ metric() }} recursively with cycle detection
    - Validates {{ custom_instructions() }} existence

    This two-pass approach ensures all references are available before resolution,
    enabling metrics to reference other metrics and proper validation of all references.
    """

    def __init__(self, enable_template_resolution: bool = True, target_database: Optional[str] = None):
        """
        Initialize the parser.

        Args:
            enable_template_resolution: Enable template resolution for Jinja-like syntax
            target_database: Optional target database to use for table references instead of SST metadata
        """
        self.error_tracker = ErrorTracker()
        self.file_detector = FileTypeDetector()
        self.enable_template_resolution = enable_template_resolution
        self.target_database = target_database
        self.manifest_parser = None  # NEW: Can be set by services for auto-detection

        # These will be initialized during parsing
        self.template_resolver: Optional[TemplateResolver] = None
        self.hardcoded_detector: Optional[HardcodedValueDetector] = None

        # Catalogs built during parsing
        self.dbt_catalog: Dict[str, Any] = {}
        self.metrics_catalog: List[Dict] = []
        self.custom_instructions_catalog: List[Dict] = []

        # Track processed files
        self.parsed_files: List[str] = []

    def parse_all_files(self, dbt_files: List[Path], semantic_files: List[Path]) -> Dict[str, Any]:
        """
        Parse all provided files with template resolution.

        This is the main entry point for parsing. It:
        1. Builds the dbt catalog from model files
        2. Collects metrics and custom instructions
        3. Resolves all template references
        4. Returns organized parsing results

        Args:
            dbt_files: List of dbt model file paths
            semantic_files: List of semantic model file paths

        Returns:
            Dictionary with parsing results organized by type
        """
        # Reset state for fresh parsing
        self._reset_state()

        # Pass 1: Build catalogs
        logger.debug("Pass 1: Building catalogs")
        self._build_dbt_catalog(dbt_files)
        self._collect_semantic_metadata(semantic_files)

        # Resolve templates in collected catalogs
        if self.enable_template_resolution:
            self._resolve_catalog_templates()

        # Initialize template resolver if enabled
        if self.enable_template_resolution and (self.dbt_catalog or self.metrics_catalog):
            self.template_resolver = TemplateResolver(
                dbt_catalog=self.dbt_catalog,
                metrics_catalog=self.metrics_catalog,
                custom_instructions_catalog=self.custom_instructions_catalog,
            )
            self.hardcoded_detector = HardcodedValueDetector(dbt_tables=set(self.dbt_catalog.keys()))

        # Pass 2: Parse with resolution
        logger.debug("Pass 2: Parsing with template resolution")
        result = {"dbt": self._parse_dbt_files(dbt_files), "semantic": self._parse_semantic_files(semantic_files)}

        # Add metadata
        result["metadata"] = {
            "parsed_files": self.parsed_files,
            "errors": self.error_tracker.get_all_errors(),
            "template_resolution_enabled": self.enable_template_resolution,
        }

        # Check for critical parsing errors that should prevent validation
        critical_errors = self.error_tracker.get_all_errors()
        if critical_errors:
            error_message = f"Critical parsing errors detected: {len(critical_errors)} error(s)"
            logger.error(error_message)
            raise ParsingCriticalError(error_message, critical_errors)

        return result

    def _reset_state(self):
        """Reset parser state for fresh parsing."""
        self.parsed_files = []
        self.dbt_catalog = {}
        self.metrics_catalog = []
        self.custom_instructions_catalog = []
        self.template_resolver = None
        self.hardcoded_detector = None
        self.error_tracker = ErrorTracker()

    def _build_dbt_catalog(self, dbt_files: List[Path]):
        """Build catalog of dbt models for reference resolution."""
        for file_path in dbt_files:
            try:
                content = file_path.read_text(encoding="utf-8")
                data = yaml.safe_load(content)

                if data and "models" in data:
                    for model in data["models"]:
                        model_name = model.get("name", "").lower()
                        if model_name:
                            self.dbt_catalog[model_name] = model

            except Exception as e:
                logger.debug(f"Error building dbt catalog from {file_path}: {e}")

    def _collect_semantic_metadata(self, semantic_files: List[Path]):
        """Collect metrics and custom instructions for template resolution."""
        for file_path in semantic_files:
            semantic_type = self.file_detector.detect_semantic_type(file_path)

            if semantic_type == "metrics":
                self._collect_metrics(file_path)
            elif semantic_type == "custom_instructions":
                self._collect_custom_instructions(file_path)

    def _collect_metrics(self, file_path: Path):
        """Collect metrics from a file for metric composition."""
        try:
            content = file_path.read_text(encoding="utf-8")

            # We need to parse the YAML while preserving the original template syntax
            # The challenge is that {{ }} breaks YAML parsing
            # So we'll use a mapping approach to restore templates after parsing

            import re

            # Create a mapping of placeholders to original templates
            template_map = {}
            counter = 0

            def replace_and_store(match):
                nonlocal counter
                placeholder = f"__TEMPLATE_{counter}__"
                template_map[placeholder] = match.group(0)
                counter += 1
                return placeholder

            # Replace templates with unique placeholders
            safe_content = re.sub(r"\{\{[^}]+\}\}", replace_and_store, content)

            # Now parse the safe YAML
            data = yaml.safe_load(safe_content)

            if data and "snowflake_metrics" in data:
                metrics = data["snowflake_metrics"]
                if isinstance(metrics, list):
                    # Restore templates in each metric
                    for metric in metrics:
                        if "expr" in metric:
                            expr = str(metric["expr"])
                            # Restore all templates in the expression
                            for placeholder, template in template_map.items():
                                expr = expr.replace(placeholder, template)
                            metric["expr"] = expr

                        # Also restore templates in other fields if needed
                        if "tables" in metric and isinstance(metric["tables"], list):
                            restored_tables = []
                            for table in metric["tables"]:
                                table_str = str(table)
                                for placeholder, template in template_map.items():
                                    table_str = table_str.replace(placeholder, template)
                                restored_tables.append(table_str)
                            metric["tables"] = restored_tables

                    # Add all metrics to catalog
                    self.metrics_catalog.extend(metrics)
                    logger.debug(f"Collected {len(metrics)} metrics from {file_path}")

        except Exception as e:
            logger.debug(f"Error collecting metrics from {file_path}: {e}")

    def _collect_custom_instructions(self, file_path: Path):
        """Collect custom instructions from a file."""
        try:
            content = file_path.read_text(encoding="utf-8")

            # Temporarily replace templates for parsing
            temp_content = self._replace_templates_for_collection(content)
            data = yaml.safe_load(temp_content)

            if data and "snowflake_custom_instructions" in data:
                instructions = data["snowflake_custom_instructions"]
                if isinstance(instructions, list):
                    # Parse the instructions to combine question_categorization and sql_generation
                    # into a single 'instruction' field that the template resolver expects
                    for instruction in instructions:
                        question_cat = instruction.get("question_categorization", "").strip()
                        sql_gen = instruction.get("sql_generation", "").strip()

                        # Combine both parts with a newline if both exist
                        combined_instruction = ""
                        if question_cat:
                            combined_instruction = question_cat
                        if sql_gen:
                            if combined_instruction:
                                combined_instruction += "\n" + sql_gen
                            else:
                                combined_instruction = sql_gen

                        parsed_instruction = {
                            "name": instruction.get("name", "").upper(),
                            "instruction": combined_instruction,
                        }
                        self.custom_instructions_catalog.append(parsed_instruction)

        except Exception as e:
            logger.debug(f"Error collecting custom instructions from {file_path}: {e}")

    def _replace_templates_for_collection(self, content: str) -> str:
        """Temporarily replace templates to allow YAML parsing."""
        # Replace templates with placeholders
        content = re.sub(r"\{\{\s*table\([^)]+\)\s*\}\}", "TEMP_TABLE", content)
        content = re.sub(r"\{\{\s*column\([^)]+\)\s*\}\}", "TEMP_COLUMN", content)
        content = re.sub(r"\{\{\s*metric\([^)]+\)\s*\}\}", "TEMP_METRIC", content)
        content = re.sub(r"\{\{\s*custom_instructions\([^)]+\)\s*\}\}", "TEMP_INSTRUCTION", content)
        return content

    def _resolve_catalog_templates(self):
        """
        Resolve templates in collected catalogs.

        Since we now preserve original templates during collection,
        we just log the catalog status here. The template resolver
        will handle the actual resolution when needed.
        """
        logger.debug(f"Collected {len(self.metrics_catalog)} metrics for template resolution")
        logger.debug(f"Collected {len(self.custom_instructions_catalog)} custom instructions for template resolution")

    def _parse_dbt_files(self, dbt_files: List[Path]) -> Dict[str, Any]:
        """Parse dbt model files."""
        from .parsers.table_summarizer import generate_table_summaries

        # For validation, we need the raw models from the catalog
        # The dbt_catalog already has all the models loaded
        result = {"models": list(self.dbt_catalog.values()), "errors": []}

        # Also parse for SST-specific semantic data
        all_parsed = {}
        for file_path in dbt_files:
            try:
                # Note: manifest_parser can be None - parse_dbt_yaml_file handles this gracefully
                parsed = dbt_parser.parse_dbt_yaml_file(
                    file_path, self.error_tracker, self.target_database, self.manifest_parser
                )
                if parsed:
                    # Merge results
                    for key, value in parsed.items():
                        if key not in all_parsed:
                            all_parsed[key] = []
                        all_parsed[key].extend(value)
                    self.parsed_files.append(str(file_path))

            except Exception as e:
                error_msg = f"Error parsing dbt file {file_path}: {e}"
                result["errors"].append(error_msg)
                self.error_tracker.add_error(f"[dbt_parsing] {error_msg}")

        # Add the parsed SST semantic data to results
        result.update(all_parsed)

        # Generate table summaries from the parsed data
        if all_parsed:
            try:
                table_summaries = generate_table_summaries(all_parsed)
                if table_summaries:
                    result["sm_table_summaries"] = table_summaries
                    logger.debug(f"Generated {len(table_summaries)} table summaries")
            except Exception as e:
                logger.warning(f"Failed to generate table summaries: {e}")

        return result

    def _parse_semantic_files(self, semantic_files: List[Path]) -> Dict[str, Any]:
        """Parse semantic model files with template resolution."""
        # Group files by semantic type
        files_by_type = self._group_files_by_type(semantic_files)

        # Parse each type
        result = {}
        for semantic_type, files in files_by_type.items():
            parsed_data = self._parse_semantic_type(semantic_type, files)
            if parsed_data:
                result[semantic_type] = parsed_data

        return result

    def _group_files_by_type(self, files: List[Path]) -> Dict[str, List[Path]]:
        """Group semantic files by their detected type."""
        grouped = {}

        for file_path in files:
            semantic_type = self.file_detector.detect_semantic_type(file_path)
            if semantic_type and semantic_type != "dbt":
                if semantic_type not in grouped:
                    grouped[semantic_type] = []
                grouped[semantic_type].append(file_path)

        return grouped

    def _parse_semantic_type(self, semantic_type: str, files: List[Path]) -> Optional[Dict[str, Any]]:
        """Parse all files of a specific semantic type."""
        all_items = []
        all_relationship_columns = []  # For relationships only
        warnings = []

        for file_path in files:
            try:
                content = file_path.read_text(encoding="utf-8")

                # Check for hardcoded values
                if self.hardcoded_detector:
                    file_warnings = self.hardcoded_detector.check_for_hardcoded_values(content, str(file_path))
                    warnings.extend(file_warnings)

                # Resolve templates if enabled
                if self.template_resolver:
                    content = self.template_resolver.resolve_content(content)

                # Parse resolved content
                parsed = self._parse_semantic_content(semantic_type, content, str(file_path))
                if parsed:
                    # Special handling for relationships which return (relationships, relationship_columns)
                    if semantic_type == "relationships" and isinstance(parsed, tuple):
                        relationships, relationship_columns = parsed
                        if relationships:
                            all_items.extend(relationships)
                        if relationship_columns:
                            all_relationship_columns.extend(relationship_columns)
                    else:
                        all_items.extend(parsed)
                    self.parsed_files.append(str(file_path))

            except ValueError as e:
                # Template resolution errors (missing metrics, circular dependencies, etc.)
                error_msg = str(e)
                logger.error(f"Template resolution error in {semantic_type} file {file_path}: {error_msg}")
                self.error_tracker.add_error(
                    f"[{semantic_type}] Template resolution failed in {file_path.name}: {error_msg}"
                )
            except Exception as e:
                # Other parsing errors
                error_msg = str(e)
                logger.error(f"Error parsing {semantic_type} file {file_path}: {error_msg}")
                self.error_tracker.add_error(f"[{semantic_type}] {error_msg}")

        # Log warnings
        for warning in warnings:
            logger.warning(warning)

        # Return special structure for relationships
        if semantic_type == "relationships":
            return (
                {"items": all_items, "relationship_columns": all_relationship_columns, "warnings": warnings}
                if all_items
                else None
            )

        return {"items": all_items, "warnings": warnings} if all_items else None

    def _parse_semantic_content(self, semantic_type: str, content: str, file_path: str) -> Optional[List[Dict]]:
        """Parse semantic content based on type."""
        try:
            data = yaml.safe_load(content)

            # Route to appropriate parser
            if semantic_type == "metrics":
                # Extract the metrics list from the data
                metrics_list = data.get("snowflake_metrics", []) if data else []
                if not metrics_list:
                    logger.warning(f"No snowflake_metrics found in {file_path}")
                    return None
                return semantic_parser.parse_snowflake_metrics(metrics_list, Path(file_path))
            elif semantic_type == "relationships":
                # Extract the relationships list from the data
                relationships_list = data.get("snowflake_relationships", []) if data else []
                if not relationships_list:
                    logger.warning(f"No snowflake_relationships found in {file_path}")
                    return None
                # parse_snowflake_relationships returns a tuple (relationships, relationship_columns)
                return semantic_parser.parse_snowflake_relationships(relationships_list, Path(file_path))
            elif semantic_type == "filters":
                filters_list = data.get("snowflake_filters", []) if data else []
                return semantic_parser.parse_snowflake_filters(filters_list, Path(file_path))
            elif semantic_type == "custom_instructions":
                instructions_list = data.get("snowflake_custom_instructions", []) if data else []
                return semantic_parser.parse_snowflake_custom_instructions(instructions_list, Path(file_path))
            elif semantic_type == "verified_queries":
                queries_list = data.get("snowflake_verified_queries", []) if data else []
                return semantic_parser.parse_snowflake_verified_queries(queries_list, Path(file_path))
            elif semantic_type == "semantic_views":
                views_list = data.get("semantic_views", []) if data else []
                if not views_list:
                    logger.warning(f"No semantic_views found in {file_path}")
                    return None
                return semantic_parser.parse_semantic_views(views_list, Path(file_path))

        except Exception as e:
            logger.error(f"Error parsing {semantic_type} content from {file_path}: {e}")

        return None
