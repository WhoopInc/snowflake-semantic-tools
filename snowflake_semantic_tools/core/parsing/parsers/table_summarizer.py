#!/usr/bin/env python3
"""
Table Summary Generator

Creates AI-optimized table summaries for Cortex Search Service.

Generates comprehensive natural language descriptions of logical tables by combining:
- Table descriptions and business context
- Alternative names (synonyms) users might search for
- Associated metrics and their descriptions
- Column information and relationships

These summaries power Cortex Search Service, enabling Cortex Analyst to better
understand user intent and map natural language queries to the correct tables.
Only tables marked with cortex_searchable=True are summarized.
"""

import ast
from typing import Any, Dict, List

from snowflake_semantic_tools.shared import get_logger

logger = get_logger("yaml_parser.table_summarizer")


def generate_table_summaries(aggregated_results: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Generate table summaries by combining descriptions, synonyms, and related metrics.
    Only generates summaries for tables where cortex_searchable=True.

    Args:
        aggregated_results: The aggregated parsing results to process

    Returns:
        List of table summary records with table_name, database_name, schema_name, table_summary, and cortex_searchable fields
    """
    logger.debug("Generating table summaries for Cortex-searchable tables...")

    # Build mappings from the parsed data (filtered to cortex_searchable=True)
    table_info_map = _build_table_info_map(aggregated_results)
    table_metrics_map = _build_table_metrics_map(aggregated_results)

    logger.debug(f"Found {len(table_info_map)} Cortex-searchable tables to summarize")

    # Generate summaries for each table
    table_summary_records = []

    for table_name, table_info in table_info_map.items():
        try:
            summary_text = _generate_summary_text(table_name, table_info, table_metrics_map)

            # Create the table summary record
            # Note: All summaries are for cortex_searchable=TRUE tables
            summary_record = {
                "table_name": table_name,
                "database_name": table_info.get("database", ""),
                "schema_name": table_info.get("schema", ""),
                "table_summary": summary_text,
                "cortex_searchable": True,
            }

            table_summary_records.append(summary_record)
            logger.debug(f"Generated summary for table {table_name}: {len(summary_text)} characters")

        except Exception as e:
            logger.error(f"Error generating summary for table {table_name}: {e}")
            continue

    logger.debug(f"Generated {len(table_summary_records)} table summaries")
    return table_summary_records


def _build_table_info_map(aggregated_results: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    """
    Build mapping of table names to their descriptions, synonyms, database, and schema.
    Only includes tables where cortex_searchable=True.

    Args:
        aggregated_results: The aggregated parsing results

    Returns:
        Dictionary mapping table_name to {description, synonyms, database, schema}
    """
    table_info_map = {}

    # Extract table information (descriptions, synonyms, database, schema) - ONLY for Cortex-searchable tables
    for table_record in aggregated_results.get("sm_tables", []):
        # Check for cortex_searchable
        cortex_searchable = table_record.get("cortex_searchable", False)

        # Skip tables not searchable
        if not cortex_searchable:
            continue

        table_name = table_record.get("table_name", "").upper()
        if table_name:
            table_info_map[table_name] = {
                "description": table_record.get("description", ""),
                "synonyms": table_record.get("synonyms", []),
                "database": table_record.get("database", ""),
                "schema": table_record.get("schema", ""),
            }

    return table_info_map


def _build_table_metrics_map(aggregated_results: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[str]]:
    """
    Build mapping of table names to their related metrics.

    Args:
        aggregated_results: The aggregated parsing results

    Returns:
        Dictionary mapping table_name to list of metric names
    """
    table_metrics_map = {}

    # Extract metrics and group by table
    for metric_record in aggregated_results.get("sm_metrics", []):
        metric_table_name = metric_record.get("table_name", "").upper()
        metric_name = metric_record.get("name", "")

        if metric_table_name and metric_name:
            # Handle case where table_name might be a stringified list
            # e.g., "['TABLE1', 'TABLE2']" -> extract individual table names
            table_names = _parse_table_names_from_metric(metric_table_name)

            # Add metric to each table it references
            for table_name in table_names:
                if table_name not in table_metrics_map:
                    table_metrics_map[table_name] = []
                table_metrics_map[table_name].append(metric_name)

    return table_metrics_map


def _parse_table_names_from_metric(metric_table_name: str) -> List[str]:
    """
    Parse table names from metric table_name field, handling both single names and stringified lists.

    Args:
        metric_table_name: The table_name field from a metric record

    Returns:
        List of individual table names
    """
    if metric_table_name.startswith("[") and metric_table_name.endswith("]"):
        # Parse the stringified list
        try:
            table_names = ast.literal_eval(metric_table_name.lower())  # Parse as lowercase first
            table_names = [name.upper() for name in table_names if isinstance(name, str)]
        except (ValueError, SyntaxError):
            # If parsing fails, treat as single table name
            table_names = [metric_table_name]
    else:
        table_names = [metric_table_name]

    return table_names


def _generate_summary_text(table_name: str, table_info: Dict[str, Any], table_metrics_map: Dict[str, List[str]]) -> str:
    """
    Generate the formatted summary text for a single table.

    Args:
        table_name: Name of the table
        table_info: Dictionary with description and synonyms
        table_metrics_map: Dictionary mapping table names to metrics

    Returns:
        Formatted summary text
    """
    summary_parts = []

    # Add description
    description = table_info.get("description", "").strip()
    if description:
        summary_parts.append(description)

    # Add synonyms
    synonyms = table_info.get("synonyms", [])
    if synonyms:
        synonyms_text = ", ".join([str(synonym) for synonym in synonyms])
        summary_parts.append(f"Here are synonyms for this table: {synonyms_text}.")

    # Add related metrics
    related_metrics = table_metrics_map.get(table_name, [])
    if related_metrics:
        metrics_text = ", ".join(related_metrics)
        summary_parts.append(f"Here are metrics that can be found in this table: {metrics_text}.")

    # Combine all parts
    if summary_parts:
        table_summary = " ".join(summary_parts)
    else:
        # Fallback if no information is available
        table_summary = f"Table: {table_name}"

    return table_summary
