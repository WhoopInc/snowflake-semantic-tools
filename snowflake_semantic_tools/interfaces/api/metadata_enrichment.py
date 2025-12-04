"""
Metadata Enrichment Python API

Provides a simple Python interface for enriching dbt YAML metadata
with semantic information from Snowflake.
"""

from typing import Any, Dict, List, Optional

from snowflake_semantic_tools.services.enrich_metadata import (
    EnrichmentConfig,
    EnrichmentResult,
    MetadataEnrichmentService,
)


class MetadataEnricher:
    """
    Python API for metadata enrichment.

    Provides a simple interface for enriching dbt YAML metadata
    with semantic information from Snowflake.

    Example:
        >>> enricher = MetadataEnricher()
        >>> result = enricher.enrich(
        ...     'models/analytics/accessories/',
        ...     database='ANALYTICS',
        ...     schema='accessories'
        ... )
        >>> print(f"Processed {result.processed}/{result.total} models")
    """

    def __init__(self, config: Optional[EnrichmentConfig] = None):
        """
        Initialize enricher with optional configuration.

        Args:
            config: Optional enrichment configuration
        """
        self.config = config
        self.service = None

    def enrich(
        self,
        target_path: str,
        database: str,
        schema: str,
        primary_key_candidates: Optional[Dict[str, List[List[str]]]] = None,
        excluded_dirs: Optional[List[str]] = None,
        dry_run: bool = False,
        fail_fast: bool = False,
    ) -> EnrichmentResult:
        """
        Enrich metadata for models at target path.

        Args:
            target_path: Path to models directory or file
            database: Target database for models (required)
            schema: Target schema for models (required)
            primary_key_candidates: Optional dict of PK candidates
            excluded_dirs: Optional list of directories to exclude
            dry_run: If True, preview changes without writing
            fail_fast: If True, stop on first error

        Returns:
            EnrichmentResult with summary and details

        Example:
            >>> enricher = MetadataEnricher()
            >>> result = enricher.enrich(
            ...     'models/analytics/memberships/',
            ...     database='ANALYTICS',
            ...     schema='memberships',
            ...     primary_key_candidates={
            ...         'membership_status_daily': [
            ...             ['user_id'],
            ...             ['user_id', 'snapshot_date']
            ...         ]
            ...     }
            ... )
        """
        # Create or update config
        if self.config:
            config = self.config
            # Override with provided parameters
            config.target_path = target_path
            config.database = database
            config.schema = schema
            if primary_key_candidates:
                config.primary_key_candidates = primary_key_candidates
            if excluded_dirs:
                config.excluded_dirs = excluded_dirs
            config.dry_run = dry_run
            config.fail_fast = fail_fast
        else:
            config = EnrichmentConfig(
                target_path=target_path,
                database=database,
                schema=schema,
                primary_key_candidates=primary_key_candidates,
                excluded_dirs=excluded_dirs,
                dry_run=dry_run,
                fail_fast=fail_fast,
            )

        # Execute enrichment
        service = MetadataEnrichmentService(config)
        service.connect()

        try:
            result = service.enrich()
            return result
        finally:
            service.close()

    def enrich_with_session(
        self,
        snowflake_session,
        target_path: str,
        database: str,
        schema: str,
        primary_key_candidates: Optional[Dict[str, List[List[str]]]] = None,
        **kwargs
    ) -> EnrichmentResult:
        """
        Enrich using existing Snowflake session.

        Useful when you already have a Snowflake connection
        and want to reuse it.

        Args:
            snowflake_session: Existing Snowflake session
            target_path: Path to models directory or file
            database: Target database for models (required)
            schema: Target schema for models (required)
            primary_key_candidates: Optional dict of PK candidates
            **kwargs: Additional enrichment options

        Returns:
            EnrichmentResult with summary and details
        """
        config = EnrichmentConfig(
            target_path=target_path,
            database=database,
            schema=schema,
            primary_key_candidates=primary_key_candidates,
            **kwargs
        )

        service = MetadataEnrichmentService(config)
        service.connect(session=snowflake_session)

        try:
            result = service.enrich()
            return result
        finally:
            service.close()
