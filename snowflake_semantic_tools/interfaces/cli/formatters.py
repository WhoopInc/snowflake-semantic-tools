"""
Output Formatters

Provides multiple output format renderers for the `sst list` command.
Supports table (terminal), JSON, YAML, and CSV output formats.
"""

import csv
import io
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import click
import yaml

_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def _sanitize_terminal(value: str) -> str:
    """Strip control characters that could manipulate terminal display."""
    return _CONTROL_CHAR_RE.sub("", value)


def format_table(headers: List[str], rows: List[List[str]], max_col_width: int = 40) -> str:
    """
    Format data as an aligned terminal table.

    Args:
        headers: Column header names
        rows: List of row values (each row is a list of strings)
        max_col_width: Maximum column width before truncation

    Returns:
        Formatted table string
    """
    if not rows:
        return "  (no items found)"

    max_col_width = max(max_col_width, 5)

    padded_rows = []
    for row in rows:
        if len(row) < len(headers):
            padded_rows.append(row + [""] * (len(headers) - len(row)))
        else:
            padded_rows.append(row)

    all_rows = [headers] + padded_rows
    col_widths = []
    for col_idx in range(len(headers)):
        max_width = 0
        for row in all_rows:
            if col_idx < len(row):
                max_width = max(max_width, len(str(row[col_idx])))
        col_widths.append(min(max_width, max_col_width))

    def format_row(row: List[str]) -> str:
        parts = []
        for col_idx, value in enumerate(row):
            width = col_widths[col_idx] if col_idx < len(col_widths) else max_col_width
            val_str = _sanitize_terminal(str(value))
            if len(val_str) > width:
                val_str = val_str[: width - 3] + "..."
            parts.append(val_str.ljust(width))
        return "  " + "  ".join(parts)

    lines = []
    lines.append(format_row(headers))
    separator = "  " + "  ".join("\u2500" * w for w in col_widths)
    lines.append(separator)
    for row in padded_rows:
        lines.append(format_row(row))

    return "\n".join(lines)


def format_json_output(data: Dict[str, Any]) -> str:
    """
    Format data as JSON with metadata.

    Args:
        data: Dictionary to serialize

    Returns:
        Pretty-printed JSON string
    """
    output = {**data, "generated_at": datetime.now(timezone.utc).isoformat()}
    return json.dumps(output, indent=2, default=str)


def format_yaml_output(data: Dict[str, Any]) -> str:
    """
    Format data as YAML.

    Args:
        data: Dictionary to serialize

    Returns:
        YAML string
    """
    return yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True)


def format_csv_output(headers: List[str], rows: List[List[str]]) -> str:
    """
    Format data as CSV (RFC 4180 compliant).

    Args:
        headers: Column header names
        rows: List of row values

    Returns:
        CSV string
    """
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    for row in rows:
        writer.writerow(row)
    return output.getvalue()


def write_output(content: str, output_file: Optional[str] = None) -> None:
    """
    Write content to file or stdout.

    Args:
        content: Content string to output
        output_file: Optional file path. If None, prints to stdout.

    Raises:
        click.ClickException: If the file cannot be written
    """
    if output_file:
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(content)
            click.echo(f"Output written to: {output_file}")
        except OSError as e:
            raise click.ClickException(f"Cannot write to '{output_file}': {e}")
    else:
        click.echo(content)
