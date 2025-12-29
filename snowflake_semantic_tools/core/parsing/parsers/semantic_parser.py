#!/usr/bin/env python3
"""
Semantic Model YAML Parser

Extracts semantic model components from YAML files for Snowflake Cortex Analyst.

Processes YAML files containing semantic model definitions and transforms them
into structured data ready for loading into Snowflake metadata tables. Each
component type maps to a specific table in the semantic model schema:

- snowflake_metrics → SM_METRICS (aggregated KPIs)
- snowflake_relationships → SM_RELATIONSHIPS (table joins)
- snowflake_filters → SM_FILTERS (WHERE conditions)
- snowflake_custom_instructions → SM_CUSTOM_INSTRUCTIONS (AI guidance)
- snowflake_verified_queries → SM_VERIFIED_QUERIES (validated examples)
- semantic_views → SM_SEMANTIC_VIEWS (domain-specific views)
"""

import re
from pathlib import Path
from typing import Any, Dict, List

import yaml

from snowflake_semantic_tools.core.parsing.parsers.dbt_parser import get_empty_result
from snowflake_semantic_tools.core.parsing.parsers.error_handler import ErrorTracker, format_yaml_error
from snowflake_semantic_tools.shared import get_logger

logger = get_logger("yaml_parser.semantic_parser")


def parse_semantic_model_file(file_path: Path, error_tracker: ErrorTracker) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse a single semantic model YAML file and extract information.

    Args:
        file_path: Path to the semantic model YAML file
        error_tracker: Error tracker to record any parsing errors

    Returns:
        Dictionary with extracted data for each table type
    """
    logger.debug(f"Parsing semantic model file: {file_path}")

    try:
        # Load YAML content
        with open(file_path, "r", encoding="utf-8") as f:
            yaml_content = yaml.safe_load(f)

        if not yaml_content:
            logger.debug(f"Empty semantic model file: {file_path}")
            return get_empty_result()

        # Process the semantic model content
        result = get_empty_result()

        # Parse different types of content
        if "snowflake_metrics" in yaml_content:
            metrics = parse_snowflake_metrics(yaml_content["snowflake_metrics"], file_path)
            result["sm_metrics"].extend(metrics)

        if "snowflake_relationships" in yaml_content:
            relationships, relationship_columns = parse_snowflake_relationships(
                yaml_content["snowflake_relationships"], file_path
            )
            result["sm_relationships"].extend(relationships)
            result["sm_relationship_columns"].extend(relationship_columns)

        if "snowflake_filters" in yaml_content:
            filters = parse_snowflake_filters(yaml_content["snowflake_filters"], file_path)
            result["sm_filters"].extend(filters)

        if "snowflake_custom_instructions" in yaml_content:
            custom_instructions = parse_snowflake_custom_instructions(
                yaml_content["snowflake_custom_instructions"], file_path
            )
            result["sm_custom_instructions"].extend(custom_instructions)

        if "snowflake_verified_queries" in yaml_content:
            verified_queries = parse_snowflake_verified_queries(yaml_content["snowflake_verified_queries"], file_path)
            result["sm_verified_queries"].extend(verified_queries)

        if "semantic_views" in yaml_content:
            semantic_views = parse_semantic_views(yaml_content["semantic_views"], file_path)
            result["sm_semantic_views"].extend(semantic_views)

        logger.debug(f"Successfully parsed semantic model file: {file_path}")
        return result

    except yaml.YAMLError as e:
        error_msg = format_yaml_error(e, file_path)
        logger.debug(error_msg)
        error_tracker.add_error(error_msg)
        return get_empty_result()

    except Exception as e:
        error_msg = f"Unexpected error parsing {file_path}: {e}"
        logger.debug(error_msg)
        error_tracker.add_error(error_msg)
        return get_empty_result()


def parse_snowflake_metrics(metrics: List[Dict[str, Any]], file_path: Path) -> List[Dict[str, Any]]:
    """Parse snowflake_metrics from semantic model files."""
    metric_records = []

    for metric in metrics:
        try:
            # Preserve the tables field for validation
            tables = metric.get("tables", [])

            # Ensure tables is a list
            if not isinstance(tables, list):
                tables = [tables] if tables else []

            # Extract primary table for backward compatibility
            if tables:
                # Take the first table as the primary table
                table_name = tables[0]

                # Safety check: If it's still a string representation of a list, handle it
                if isinstance(table_name, str) and table_name.startswith("["):
                    logger.warning(f"Metric '{metric.get('name', '')}' has unresolved table reference: {table_name}")
                    table_name = ""
            else:
                table_name = ""

            metric_record = {
                "name": metric.get("name", "").upper(),
                "table_name": table_name.upper() if isinstance(table_name, str) else str(table_name).upper(),
                "tables": tables,  # PRESERVE THE FULL TABLES ARRAY
                "description": metric.get("description", ""),
                "expr": metric.get("expr", ""),
                "synonyms": metric.get("synonyms", []),
                "sample_values": metric.get("sample_values", []),
            }
            metric_records.append(metric_record)

        except Exception as e:
            logger.error(f"Error parsing metric in {file_path}: {e}")
            continue

    return metric_records


def parse_snowflake_relationships(
    relationships: List[Dict[str, Any]], file_path: Path
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Parse snowflake_relationships from semantic model files.

    Returns:
        Tuple of (relationship_records, relationship_column_records)
    """
    relationship_records = []
    relationship_column_records = []

    for relationship in relationships:
        try:
            # Main relationship record with uppercase formatting
            rel_record = {
                "relationship_name": relationship.get("name", "").upper(),
                "left_table_name": relationship.get("left_table", "").upper(),
                "right_table_name": relationship.get("right_table", "").upper(),
            }
            relationship_records.append(rel_record)

            # Parse relationship conditions (new format)
            from snowflake_semantic_tools.core.parsing.join_condition_parser import JoinConditionParser

            relationship_conditions = relationship.get("relationship_conditions", [])
            for condition in relationship_conditions:
                # Parse the condition to extract details
                parsed = JoinConditionParser.parse(condition)

                # Build fully qualified column references for validation
                left_col_qualified = (
                    f"{parsed.left_table}.{parsed.left_column}" if parsed.left_table and parsed.left_column else ""
                )
                right_col_qualified = (
                    f"{parsed.right_table}.{parsed.right_column}" if parsed.right_table and parsed.right_column else ""
                )

                rel_col_record = {
                    "relationship_name": relationship.get("name", "").upper(),
                    "join_condition": condition,
                    "condition_type": parsed.condition_type.value,
                    "left_expression": parsed.left_expression,
                    "right_expression": parsed.right_expression,
                    "left_column": left_col_qualified,  # For backward compatibility with validation
                    "right_column": right_col_qualified,  # For backward compatibility with validation
                    "operator": parsed.operator,
                }
                relationship_column_records.append(rel_col_record)

        except Exception as e:
            logger.error(f"Error parsing relationship in {file_path}: {e}")
            continue

    return relationship_records, relationship_column_records


def parse_snowflake_filters(filters: List[Dict[str, Any]], file_path: Path) -> List[Dict[str, Any]]:
    """Parse snowflake_filters from semantic model files."""
    filter_records = []

    for filter_item in filters:
        try:
            expr = filter_item.get("expr", "")

            # Try explicit tables field first (backward compatibility)
            tables = filter_item.get("tables", [])
            if isinstance(tables, list) and tables:
                # Handle {{ table('name') }} template syntax
                table_name = _extract_table_name_from_template(tables[0])
            else:
                # Extract table names from {{ column('table', 'col') }} expressions in expr
                table_names = _extract_table_names_from_jinja(expr)
                table_name = table_names[0] if table_names else ""

            filter_record = {
                "name": filter_item.get("name", "").upper(),
                "table_name": table_name.upper() if table_name else "",
                "description": filter_item.get("description", ""),
                "expr": expr,
                "synonyms": filter_item.get("synonyms", []),
            }
            filter_records.append(filter_record)

        except Exception as e:
            logger.error(f"Error parsing filter in {file_path}: {e}")
            continue

    return filter_records


def _extract_table_names_from_jinja(expr: str) -> List[str]:
    """
    Extract table names from {{ column('table', 'col') }} expressions.

    Args:
        expr: Expression string potentially containing Jinja2 column references

    Returns:
        List of unique table names found in the expression

    Examples:
        >>> _extract_table_names_from_jinja("{{ column('orders', 'total') }} > 0")
        ['orders']
        >>> _extract_table_names_from_jinja("{{ column('orders', 'a') }} AND {{ column('users', 'b') }}")
        ['orders', 'users']
    """
    # Pattern: {{ column('table_name', 'column_name') }} - handles both single and double quotes
    pattern = r"{{\s*column\s*\(\s*['\"]([^'\"]+)['\"]"
    matches = re.findall(pattern, expr)
    # Return unique table names while preserving first occurrence order
    seen = set()
    unique_tables = []
    for match in matches:
        table = match.strip()
        if table and table not in seen:
            seen.add(table)
            unique_tables.append(table)
    return unique_tables


def _extract_table_name_from_template(template: str) -> str:
    """
    Extract table name from {{ table('name') }} template.

    Args:
        template: Template string like "{{ table('orders') }}"

    Returns:
        Extracted table name or original string if not a template

    Examples:
        >>> _extract_table_name_from_template("{{ table('orders') }}")
        'orders'
        >>> _extract_table_name_from_template("orders")
        'orders'
    """
    pattern = r"{{\s*table\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*}}"
    match = re.search(pattern, template)
    return match.group(1) if match else template


def parse_snowflake_custom_instructions(instructions: List[Dict[str, Any]], file_path: Path) -> List[Dict[str, Any]]:
    """Parse snowflake_custom_instructions from semantic model files."""
    instruction_records = []

    for instruction in instructions:
        try:
            # Combine question_categorization and sql_generation into a single instruction field
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

            instruction_record = {
                "name": instruction.get("name", "").upper(),
                "instruction": combined_instruction,
            }
            instruction_records.append(instruction_record)

        except Exception as e:
            logger.error(f"Error parsing custom instruction in {file_path}: {e}")
            continue

    return instruction_records


def parse_snowflake_verified_queries(queries: List[Dict[str, Any]], file_path: Path) -> List[Dict[str, Any]]:
    """Parse snowflake_verified_queries from semantic model files."""
    query_records = []

    for query in queries:
        try:
            # Keep tables as list for verified queries since they might reference multiple tables
            query_record = {
                "name": query.get("name", "").upper(),
                "question": query.get("question", ""),
                "tables": query.get("tables", []),
                "verified_at": query.get("verified_at", ""),
                "verified_by": query.get("verified_by", ""),
                "use_as_onboarding_question": query.get("use_as_onboarding_question", ""),
                "sql": query.get("sql", ""),
            }
            query_records.append(query_record)

        except Exception as e:
            logger.error(f"Error parsing verified query in {file_path}: {e}")
            continue

    return query_records


def parse_semantic_views(semantic_views: List[Dict[str, Any]], file_path: Path) -> List[Dict[str, Any]]:
    """Parse semantic_views from semantic model files."""
    import json

    view_records = []

    for view_def in semantic_views:
        try:
            if not isinstance(view_def, dict):
                logger.error(f"Semantic view definition must be a dictionary, got {type(view_def)} in {file_path}")
                continue

            # Extract required fields
            name = view_def.get("name")
            if not name:
                logger.error(f"Semantic view definition missing required 'name' field in {file_path}")
                continue

            description = view_def.get("description", "")

            tables = view_def.get("tables", [])
            if not isinstance(tables, list):
                logger.error(f"'tables' must be a list for view '{name}' in {file_path}, got {type(tables)}")
                continue

            if not tables:
                logger.error(f"Semantic view '{name}' must have at least one table in {file_path}")
                continue

            # Convert tables list to JSON string for storage
            tables_json = json.dumps(tables)

            # Extract custom_instructions if present
            custom_instructions = view_def.get("custom_instructions", [])
            if not isinstance(custom_instructions, list):
                custom_instructions = [custom_instructions] if custom_instructions else []

            # Process custom_instructions - join multiple instructions with newlines
            # Note: At this point, template references should already be resolved
            # to actual instruction text by the template resolver
            if custom_instructions and len(custom_instructions) > 0:
                # Join multiple instructions with double newline for readability
                custom_instructions_text = "\n\n".join(str(inst) for inst in custom_instructions if inst)
                # Only store if we have actual content
                custom_instructions_json = custom_instructions_text if custom_instructions_text.strip() else None
            else:
                custom_instructions_json = None

            view_record = {
                "name": str(name),
                "description": str(description),
                "tables": tables_json,
                "custom_instructions": custom_instructions_json,
            }

            view_records.append(view_record)
            logger.debug(f"Parsed semantic view: {name} with {len(tables)} table(s)")

        except Exception as e:
            logger.error(f"Error parsing semantic view in {file_path}: {e}")
            continue

    return view_records


def parse_multiple_semantic_files(
    file_paths: List[Path], error_tracker: ErrorTracker
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse multiple semantic model YAML files and aggregate results.

    Args:
        file_paths: List of file paths to parse
        error_tracker: Error tracker to record any parsing errors

    Returns:
        Aggregated results from all files
    """
    logger.info(f"Parsing {len(file_paths)} semantic model files")

    # Initialize aggregated results
    aggregated_results = get_empty_result()

    # Parse each file
    for file_path in file_paths:
        try:
            file_results = parse_semantic_model_file(file_path, error_tracker)

            # Aggregate results
            for table_type, records in file_results.items():
                aggregated_results[table_type].extend(records)

        except Exception as e:
            error_msg = f"Failed to parse semantic file {file_path}: {e}"
            logger.error(error_msg)
            error_tracker.add_error(error_msg)

    return aggregated_results
