"""
Base classes and fixtures for integration tests.

This module provides common fixtures and utilities for integration testing
using the embedded sample project.
"""

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import pytest

from tests.fixtures.sample_project_factory import SampleProjectFactory


class IntegrationTestBase:
    """Base class for integration tests with sample project support."""

    @pytest.fixture
    def sample_project(self, tmp_path: Path) -> Path:
        """
        Provide a clean sample project for each test.

        Returns:
            Path to a fresh copy of the sample project
        """
        return SampleProjectFactory.create(tmp_path)

    @pytest.fixture
    def uninitialized_project(self, tmp_path: Path) -> Path:
        """
        Provide a dbt project without SST setup.

        Useful for testing the init wizard.

        Returns:
            Path to a dbt project without SST configuration
        """
        return SampleProjectFactory.create_without_sst(tmp_path)

    @pytest.fixture
    def unenriched_project(self, tmp_path: Path) -> Path:
        """
        Provide a project without SST metadata for enrichment tests.

        Returns:
            Path to a project with models that have no SST metadata
        """
        return SampleProjectFactory.create_unenriched(tmp_path)

    @pytest.fixture
    def project_with_profiles(self, tmp_path: Path) -> Path:
        """
        Provide a project with profiles.yml configured.

        Returns:
            Path to a project with profiles.yml
        """
        return SampleProjectFactory.create_with_profiles(tmp_path)

    @pytest.fixture
    def project_context(self, sample_project: Path) -> Generator[Path, None, None]:
        """
        Context manager for running tests in project directory.

        Changes the working directory to the sample project for the
        duration of the test, then restores the original directory.

        Yields:
            Path to the sample project (current working directory)
        """
        original_cwd = os.getcwd()
        os.chdir(sample_project)
        try:
            yield sample_project
        finally:
            os.chdir(original_cwd)

    @staticmethod
    @contextmanager
    def working_directory(path: Path) -> Generator[Path, None, None]:
        """
        Context manager for temporarily changing working directory.

        Args:
            path: Directory to change to

        Yields:
            The path that was changed to
        """
        original_cwd = os.getcwd()
        os.chdir(path)
        try:
            yield path
        finally:
            os.chdir(original_cwd)
