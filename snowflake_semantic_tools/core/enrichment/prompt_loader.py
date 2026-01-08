#!/usr/bin/env python3
"""
Prompt Loader

Utility for loading and rendering LLM prompt templates from external files.
Prompts are stored as markdown files in the prompts/ directory for better
maintainability and iteration.
"""

from functools import lru_cache
from pathlib import Path

PROMPTS_DIR = Path(__file__).parent / "prompts"


@lru_cache(maxsize=10)
def load_prompt(name: str) -> str:
    """
    Load a prompt template from file.

    Templates are cached for performance.

    Args:
        name: Prompt template name (without .md extension)

    Returns:
        Raw template content

    Raises:
        FileNotFoundError: If template doesn't exist
    """
    path = PROMPTS_DIR / f"{name}.md"
    if not path.exists():
        raise FileNotFoundError(f"Prompt template not found: {path}")
    return path.read_text()


def render_prompt(name: str, **variables) -> str:
    """
    Load and render a prompt template with variables.

    Args:
        name: Prompt template name (without .md extension)
        **variables: Variables to substitute in the template

    Returns:
        Rendered prompt string

    Raises:
        FileNotFoundError: If template doesn't exist
        KeyError: If a required variable is missing
    """
    template = load_prompt(name)
    return template.format(**variables)


def clear_cache() -> None:
    """Clear the prompt template cache."""
    load_prompt.cache_clear()
