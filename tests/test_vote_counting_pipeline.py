"""Tests for the VoteCountingPipeline class."""

import json
from datetime import datetime
from typing import Any, Type
from unittest.mock import MagicMock, patch
import pytest

from vote_counter.vote_counter import VoteCountingPipeline
from vote_counter.config import Config
from vote_counter.graphql_client import GraphQLClient


class TestConfig:
    """Base configuration for test cases."""

    class Dates:
        """Date constants for testing."""

        START_DATE: str
        END_DATE: str
        FORMAT: str = "%Y-%m-%d %H:%M:%S.%f%z"

    class Files:
        """File path constants for testing."""

        GRAPHQL_RESPONSE: str
        VOTE_COUNT: str

    TEST_CASE: int

    @classmethod
    def set_file_paths(cls):
        cls.Files.GRAPHQL_RESPONSE = (
            f"tests/resources/graphql/response/{cls.TEST_CASE}.json"
        )
        cls.Files.VOTE_COUNT = f"tests/resources/vote/count/{cls.TEST_CASE}.json"


class TestCase1(TestConfig):
    """Configuration for test case 1."""

    class Dates:
        START_DATE: str = "2024-08-10 16:24:59.689550+00:00"
        END_DATE: str = "2024-08-31 23:59:59.689550+00:00"
        FORMAT: str = "%Y-%m-%d %H:%M:%S.%f%z"

    TEST_CASE: int = 1


class TestCase2(TestCase1):
    TEST_CASE: int = 2


class TestCase3(TestCase1):
    TEST_CASE: int = 3


class TestCase4(TestCase1):
    TEST_CASE: int = 4


@pytest.fixture
def mock_config() -> Config:
    """Fixture for mocking the Config class."""
    return Config()


@pytest.fixture
def mock_graphql_client() -> MagicMock:
    """Fixture for mocking the GraphQLClient."""
    return MagicMock(spec=GraphQLClient)


@pytest.fixture
def vote_counting_pipeline(
    mock_config: Config, mock_graphql_client: MagicMock, test_config: Type[TestConfig]
) -> VoteCountingPipeline:
    """Fixture for creating a VoteCountingPipeline instance."""
    test_config.set_file_paths()
    start_date = datetime.strptime(
        test_config.Dates.START_DATE, test_config.Dates.FORMAT
    )
    end_date = datetime.strptime(test_config.Dates.END_DATE, test_config.Dates.FORMAT)

    return VoteCountingPipeline(start_date, end_date, mock_graphql_client, mock_config)


@pytest.mark.parametrize("test_config", [TestCase1, TestCase2, TestCase3, TestCase4])
def test_vote_counting_pipeline_run(
    test_config: Type[TestConfig],
    vote_counting_pipeline: VoteCountingPipeline,
    mock_graphql_client: MagicMock,
) -> None:
    """
    Test that the VoteCountingPipeline.run() method produces correct results.

    Args:
        test_config: The test configuration class.
        vote_counting_pipeline: VoteCountingPipeline instance.
        mock_graphql_client: Mocked GraphQLClient instance.
    """
    # GIVEN
    with open(test_config.Files.GRAPHQL_RESPONSE, "r") as f:
        mock_graphql_response: dict[str, Any] = json.load(f)["data"]

    with open(test_config.Files.VOTE_COUNT, "r") as f:
        expected_vote_counts: dict[str, dict[str, Any]] = json.load(f)

    mock_graphql_client.execute_query.return_value = mock_graphql_response

    # WHEN
    with patch.object(VoteCountingPipeline, "save_results") as mock_save_results:
        saved_vote_counts = vote_counting_pipeline.run()

    # THEN
    mock_save_results.assert_called_once()

    assert saved_vote_counts == expected_vote_counts, (
        f"Vote counts do not match for test case {test_config.TEST_CASE}.\n"
        f"Expected: {expected_vote_counts}\n"
        f"Actual: {saved_vote_counts}"
    )

    mock_graphql_client.execute_query.assert_called_once()
    assert vote_counting_pipeline.start_date == datetime.strptime(
        test_config.Dates.START_DATE, test_config.Dates.FORMAT
    )
    assert vote_counting_pipeline.end_date == datetime.strptime(
        test_config.Dates.END_DATE, test_config.Dates.FORMAT
    )
