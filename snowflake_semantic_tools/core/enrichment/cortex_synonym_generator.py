#!/usr/bin/env python3
"""
Cortex Synonym Generator

Generates natural language synonyms for tables and columns using Snowflake Cortex LLM.
Supports batch processing for performance.
"""

import json
import re
from typing import Any, Dict, List, Optional, Union

from snowflake_semantic_tools.shared.utils import get_logger
from snowflake_semantic_tools.shared.utils.character_sanitizer import CharacterSanitizer

logger = get_logger(__name__)


class CortexSynonymGenerator:
    """Generate synonyms using Snowflake Cortex Complete LLM."""

    def __init__(self, snowflake_client, model: str = "openai-gpt-4.1", max_synonyms: int = 4):
        """
        Initialize synonym generator.

        Args:
            snowflake_client: SnowflakeClient instance for Cortex access
            model: Cortex model to use (default: openai-gpt-4.1)
            max_synonyms: Maximum synonyms to generate per item (default: 4)
        """
        self.snowflake_client = snowflake_client
        self.model = model
        self.max_synonyms = max_synonyms
        self._cortex_verified = False  # Track if Cortex has been verified

        logger.info(f"Initialized Cortex synonym generator with model: {model}")

    def generate_table_synonyms(
        self,
        table_name: str,
        description: str,
        column_info: List[Dict[str, Any]],
        existing_synonyms: Optional[List[str]] = None,
        full_yaml_context: Optional[str] = None,
        force: bool = False,
    ) -> List[str]:
        """
        Generate table-level synonyms.

        Args:
            table_name: Table name
            description: Table description
            column_info: List of column dictionaries
            existing_synonyms: Existing synonyms (preserved unless force=True)
            full_yaml_context: Complete YAML for better context
            force: Regenerate even if synonyms exist

        Returns:
            List of synonym strings
        """
        if existing_synonyms and len(existing_synonyms) > 0 and not force:
            logger.debug(f"Table {table_name} already has synonyms, skipping")
            return existing_synonyms

        # Build context
        if full_yaml_context:
            context = f"Complete YAML definition:\n{full_yaml_context[:2000]}"
        else:
            context = self._build_column_context(column_info[:10])

        prompt = self._build_table_synonym_prompt(table_name, description, context)

        try:
            response = self._execute_cortex(prompt)
            synonyms = self._parse_response_as_list(response, f"table {table_name}")
            return CharacterSanitizer.sanitize_synonym_list(synonyms)
        except RuntimeError as e:
            # Cortex access/permission error - surface prominently
            logger.warning(f"   Cortex unavailable for table '{table_name}': {e}")
            logger.warning(
                "   Synonym generation will be skipped. Run with --synonyms later once Cortex access is configured."
            )
            return []
        except Exception as e:
            logger.error(f"Failed to generate table synonyms for {table_name}: {e}")
            return []

    def generate_column_synonyms_batch(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        table_description: Optional[str] = None,
        full_yaml_context: Optional[str] = None,
        force: bool = False,
    ) -> Dict[str, List[str]]:
        """
        Generate synonyms for ALL columns in a single Cortex call.

        Args:
            table_name: Table name
            columns: List of column dicts
            table_description: Table description
            full_yaml_context: Full YAML context
            force: Force regeneration

        Returns:
            Dict mapping column_name -> list of synonyms
        """
        prompt = self._build_batch_column_synonym_prompt(table_name, columns, table_description, full_yaml_context)

        try:
            response = self._execute_cortex(prompt)
            result = self._parse_response_as_dict(response, f"batch columns for {table_name}")

            # Sanitize all synonym lists
            return {col: CharacterSanitizer.sanitize_synonym_list(syns) for col, syns in result.items()}
        except RuntimeError as e:
            # Cortex access/permission error - surface prominently
            logger.warning(f"   Cortex unavailable for column synonyms in '{table_name}': {e}")
            return {}
        except Exception as e:
            logger.error(f"Batch column synonyms failed for {table_name}: {e}")
            return {}

    # Core Cortex interaction methods

    def _verify_cortex_access(self) -> None:
        """
        Verify Cortex access on first call with a simple test.

        Raises clear error message if Cortex is unavailable.
        """
        if self._cortex_verified:
            return

        test_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{self.model}',
            'Say hello'
        ) as RESPONSE
        """

        try:
            result = self.snowflake_client.execute_query(test_query)
            if result.empty:
                raise RuntimeError("Cortex returned empty response")
            self._cortex_verified = True
            logger.debug(f"Cortex access verified with model: {self.model}")
        except Exception as e:
            error_msg = str(e).lower()
            if "access" in error_msg or "permission" in error_msg or "privilege" in error_msg:
                raise RuntimeError(
                    f"Cortex permission error: {e}\n"
                    f"Ensure your role has access to SNOWFLAKE.CORTEX.COMPLETE function.\n"
                    f"Try: GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE <your_role>;"
                ) from e
            elif "model" in error_msg or "not found" in error_msg:
                raise RuntimeError(
                    f"Cortex model '{self.model}' not available: {e}\n"
                    f"Available models: llama3.2-3b, mistral-large2, openai-gpt-4.1"
                ) from e
            else:
                raise RuntimeError(
                    f"Cortex connection failed: {e}\n" f"Check your Snowflake connection and Cortex availability."
                ) from e

    def _execute_cortex(self, prompt: str) -> str:
        """
        Execute Cortex Complete and return raw response.

        Args:
            prompt: The prompt to send to Cortex

        Returns:
            Raw response text from Cortex

        Raises:
            RuntimeError: If Cortex call fails with descriptive error
        """
        # Verify Cortex access on first call
        self._verify_cortex_access()

        escaped_prompt = prompt.replace("'", "''")
        query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{self.model}',
            '{escaped_prompt}'
        ) as RESPONSE
        """

        try:
            result = self.snowflake_client.execute_query(query)
        except Exception as e:
            raise RuntimeError(f"Cortex query failed: {e}") from e

        if result.empty:
            raise RuntimeError("Cortex returned empty response - check model availability")

        return result.iloc[0]["RESPONSE"]

    def _parse_response_as_list(self, response_text: str, context: str) -> List[str]:
        """
        Parse Cortex response as list of synonyms.

        Args:
            response_text: Raw Cortex response
            context: Context for logging (e.g., "table users")

        Returns:
            List of synonym strings
        """
        # Use robust JSON extraction (handles markdown fences, preamble, etc.)
        cleaned_text = self._extract_json_from_response(response_text)

        try:
            response_obj = json.loads(cleaned_text)

            # Try different JSON structures
            if isinstance(response_obj, list):
                return response_obj
            if isinstance(response_obj, dict) and "synonyms" in response_obj:
                return response_obj["synonyms"]

            logger.warning(f"Unexpected JSON structure for {context}, trying text extraction")
            return self._extract_synonyms_from_text(response_text, self.max_synonyms, context)

        except json.JSONDecodeError:
            logger.debug(f"Non-JSON response for {context}, extracting from text")
            return self._extract_synonyms_from_text(response_text, self.max_synonyms, context)

    def _parse_response_as_dict(self, response_text: str, context: str) -> Dict[str, List[str]]:
        """
        Parse Cortex response as dict of column -> synonyms.

        Args:
            response_text: Raw Cortex response
            context: Context for logging

        Returns:
            Dict mapping column names to synonym lists
        """
        cleaned_text = self._extract_json_from_response(response_text)

        try:
            response_obj = json.loads(cleaned_text)

            # Try different JSON structures
            if isinstance(response_obj, dict) and "columns" in response_obj:
                return response_obj["columns"]
            if isinstance(response_obj, dict):
                return response_obj

            logger.warning(f"Unexpected JSON structure for {context}")
            return {}

        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON for {context}")
            return {}

    def _extract_json_from_response(self, response_text: str) -> str:
        """
        Extract JSON from LLM response, handling various formats.

        Handles:
        - Raw JSON
        - Markdown code fences (```json ... ```, ``` ... ```)
        - Preamble/trailing text around JSON
        - Uppercase/lowercase fence labels

        Args:
            response_text: Raw LLM response

        Returns:
            Cleaned JSON string ready for parsing
        """
        if not response_text:
            return ""

        text = response_text.strip()

        # Strategy 1: Find JSON by locating outermost braces
        # This handles preamble text, markdown fences, and trailing explanations
        first_brace = text.find("{")
        last_brace = text.rfind("}")

        if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
            return text[first_brace : last_brace + 1]

        # Strategy 2: Handle array responses (e.g., ["synonym1", "synonym2"])
        first_bracket = text.find("[")
        last_bracket = text.rfind("]")

        if first_bracket != -1 and last_bracket != -1 and last_bracket > first_bracket:
            return text[first_bracket : last_bracket + 1]

        # Fallback: return as-is and let JSON parser handle it
        return text

    def _extract_synonyms_from_text(self, text: str, max_results: int, context_name: str) -> List[str]:
        """
        Extract synonyms from non-JSON text response (fallback).

        Finds quoted strings in text and returns them as synonyms.
        """
        synonyms = []

        # Try to find quoted strings
        quoted_pattern = r'["\']([^"\']+)["\']'
        matches = re.findall(quoted_pattern, text)

        for match in matches:
            if match and len(match) > 2:
                synonyms.append(match.strip())
                if len(synonyms) >= max_results:
                    break

        if synonyms:
            logger.debug(f"Extracted {len(synonyms)} synonyms from text for {context_name}")
            return synonyms[:max_results]

        return []

    # Prompt building methods

    def _build_table_synonym_prompt(self, table_name: str, description: str, full_context: str) -> str:
        """Build prompt for table synonym generation."""
        readable_name = table_name.lower().replace("int_", "").replace("_", " ")

        return f"""You are analyzing a database table to generate natural language synonyms that will help analysts discover this data through conversational queries.

TABLE INFORMATION:
Name: {table_name}
Readable: {readable_name}
Description: {description[:800] if description else "No description provided"}

COMPLETE TABLE DEFINITION:
{full_context[:1500]}

TASK:
Generate up to {self.max_synonyms} natural language synonyms that describe what this table contains or what questions it helps answer. These will be used for semantic search and natural language queries.

GOOD EXAMPLES (generic, descriptive):
- "customer transaction history"
- "daily sales performance metrics"
- "product inventory status tracking"
- "employee performance reviews"
- "web session clickstream data"

BAD EXAMPLES:
- "{readable_name}" (just repeating table name in plain English)
- "{table_name}" (technical table name format)
- "data table" (too generic/vague)
- "database information" (not specific enough)

REQUIREMENTS:
- Natural, conversational language (as if describing the table to a colleague)
- Brief but descriptive (typically 3-6 words, can be longer if needed for clarity)
- Focus on what the data represents or what business questions it answers
- NO technical formatting (snake_case, prefixes like int_, etc.)
- Think: "If someone asked 'where's the data on X?', what would they say?"

Return ONLY a JSON array of strings: ["synonym 1", "synonym 2", "synonym 3"]"""

    def _build_batch_column_synonym_prompt(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        table_description: Optional[str] = None,
        yaml_context: Optional[str] = None,
    ) -> str:
        """Build batch prompt for ALL columns at once."""

        # Build column list
        column_lines = []
        for col in columns[:50]:  # Limit to first 50 to avoid token limits
            col_name = col.get("name", "")
            col_desc = col.get("description", "No description")[:200]

            meta_sst = col.get("meta", {}).get("sst", {})
            data_type = meta_sst.get("data_type", "unknown")
            samples = meta_sst.get("sample_values", [])
            sample_str = ", ".join([str(s) for s in samples[:3]]) if samples else ""

            column_lines.append(f"  - {col_name}: {col_desc} (type: {data_type}, samples: {sample_str})")

        columns_text = "\n".join(column_lines)

        return f"""You are analyzing database columns to generate natural language synonyms for ALL columns at once.

TABLE: {table_name}
Description: {table_description[:300] if table_description else 'No description'}

COLUMNS TO PROCESS:
{columns_text}

FULL CONTEXT (first 800 chars):
{yaml_context[:800] if yaml_context else 'Not available'}

TASK:
Generate up to {self.max_synonyms} natural language synonyms for EACH column listed above.

IMPORTANT - Return as a single JSON object:
{{
  "column_name_1": ["synonym 1", "synonym 2", ...],
  "column_name_2": ["synonym 1", "synonym 2", ...],
  ...
}}

GOOD SYNONYM EXAMPLES:
- "transaction timestamp"
- "customer unique identifier"
- "total purchase amount"
- "primary contact email"

BAD EXAMPLES:
- "table_name.column_name" (NO table prefix)
- "column_name" (just repeating name)
- "varchar" (just data type)

REQUIREMENTS:
- Natural, conversational language
- 2-4 words typically
- NO table name prefix
- NO snake_case
- Focus on what data the column contains

Return ONLY the JSON object with all column synonyms."""

    def _build_column_context(self, columns: List[Dict[str, Any]]) -> str:
        """
        Build context string from columns.

        Args:
            columns: List of column dictionaries

        Returns:
            Formatted context string
        """
        context_lines = []

        for col in columns:
            col_name = col.get("name", "unknown")
            col_desc = col.get("description", "No description")

            meta_sst = col.get("meta", {}).get("sst", {})
            samples = meta_sst.get("sample_values", [])
            sample_str = ""
            if samples:
                sample_str = f" (samples: {', '.join([str(s) for s in samples[:3]])})"

            line = f"- {col_name}: {col_desc[:100]}{sample_str}"
            context_lines.append(line)

        return "\n".join(context_lines)
