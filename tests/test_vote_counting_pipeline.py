"""Tests for the VoteCountingPipeline class."""

import json
from datetime import datetime, timezone
from typing import Any, Type
from unittest.mock import MagicMock, patch
import pytest
import pprint

from gqa.graphql_query_aggregator import GraphQLQueryAggregator, BlockDiscontinuityError
from vote_counter.vote_counter import VoteCountingPipeline
from vote_counter.config import Config


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
        START_DATE: str = "2024-09-01 00:00:00.689550+00:00"
        END_DATE: str = "2024-09-10 23:59:59.999550+00:00"
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
def mock_gqa() -> MagicMock:
    """Fixture for mocking the GraphQLQueryAggregator."""
    return MagicMock(spec=GraphQLQueryAggregator)


@pytest.fixture
def vote_counting_pipeline(
    mock_config: Config, mock_gqa: MagicMock, test_config: Type[TestConfig]
) -> VoteCountingPipeline:
    """Fixture for creating a VoteCountingPipeline instance."""
    test_config.set_file_paths()
    start_date = datetime.strptime(
        test_config.Dates.START_DATE, test_config.Dates.FORMAT
    )
    end_date = datetime.strptime(test_config.Dates.END_DATE, test_config.Dates.FORMAT)

    return VoteCountingPipeline(start_date, end_date, mock_gqa, mock_config)


def load_json_file(file_path: str) -> Any:
    with open(file_path, "r") as f:
        return json.load(f)


@pytest.mark.parametrize("test_config", [TestCase1, TestCase2, TestCase3, TestCase4])
def test_vote_counting_pipeline_run(
    test_config: Type[TestConfig],
    vote_counting_pipeline: VoteCountingPipeline,
    mock_gqa: MagicMock,
) -> None:
    """
    Test that the VoteCountingPipeline.run() method produces correct results.

    Args:
        test_config: The test configuration class.
        vote_counting_pipeline: VoteCountingPipeline instance.
        mock_gqa: Mocked GraphQLQueryAggregator instance.
    """
    # GIVEN
    with open(test_config.Files.GRAPHQL_RESPONSE, "r") as f:
        response_raw: dict[str, dict[str, Any]] = json.load(f)["data"]
        mock_transactions = GraphQLQueryAggregator._get_transactions_from_response(
            [response_raw],
            vote_counting_pipeline.start_date,
            vote_counting_pipeline.end_date,
            0,
        )

    with open(test_config.Files.VOTE_COUNT, "r") as f:
        expected_vote_counts: dict[str, dict[str, Any]] = json.load(f)

    mock_gqa.retrieve_combined_transactions.return_value = mock_transactions

    # WHEN
    with patch.object(VoteCountingPipeline, "save_results") as mock_save_results:
        saved_vote_counts = vote_counting_pipeline.run()

    # THEN
    mock_save_results.assert_called_once()
    mock_gqa.retrieve_combined_transactions.assert_called_once_with(
        vote_counting_pipeline.start_date, vote_counting_pipeline.end_date
    )

    assert compare_vote_counts(saved_vote_counts, expected_vote_counts), (
        f"Vote counts do not match for test case {test_config.TEST_CASE}.\n"
        f"Expected: {expected_vote_counts}\n"
        f"Actual: {saved_vote_counts}"
    )


def compare_vote_counts(actual: dict, expected: dict) -> bool:
    if set(actual.keys()) != set(expected.keys()):
        return False
    for key in actual:
        if set(actual[key].keys()) != set(expected[key].keys()):
            return False
        for vote_type in actual[key]:
            if actual[key][vote_type]["count"] != expected[key][vote_type]["count"]:
                return False
            if set(actual[key][vote_type]["addresses"]) != set(
                expected[key][vote_type]["addresses"]
            ):
                return False
    return True 


@pytest.mark.parametrize("test_config", [TestCase1, TestCase2, TestCase3, TestCase4])
def test_vote_counting_pipeline(
    mock_gqa: MagicMock,
    mock_config: Config,
    test_config: Type[TestConfig],
    vote_counting_pipeline: VoteCountingPipeline,
):
    with open(
        f"tests/resources/graphql/response/{test_config.TEST_CASE}.json", "r"
    ) as f:
        response_raw: dict[str, dict[str, Any]] = json.load(f)["data"]
        mock_transactions = GraphQLQueryAggregator._get_transactions_from_response(
            [response_raw],
            vote_counting_pipeline.start_date,
            vote_counting_pipeline.end_date,
            0,
        )

    expected_vote_counts = load_json_file(
        f"tests/resources/vote/count/{test_config.TEST_CASE}.json"
    )

    # Prepare mock GQA
    mock_gqa.retrieve_combined_transactions.return_value = mock_transactions

    # Run the pipeline
    vote_counts = vote_counting_pipeline.run()

    # Assert the results
    assert vote_counts == expected_vote_counts


def test_block_discontinuity(mock_gqa: MagicMock, mock_config: Config):
    # Prepare mock GQA to raise BlockDiscontinuityError
    mock_gqa.retrieve_combined_transactions.side_effect = BlockDiscontinuityError(
        "Test discontinuity"
    )

    # Create VoteCountingPipeline
    start_date = datetime(2024, 8, 10, 16, 24, 59, 689550, tzinfo=timezone.utc)
    end_date = datetime(2024, 9, 10, 20, 24, 59, 689550, tzinfo=timezone.utc)
    pipeline = VoteCountingPipeline(start_date, end_date, mock_gqa, mock_config)

    # Run the pipeline and expect it to raise BlockDiscontinuityError
    with pytest.raises(BlockDiscontinuityError):
        pipeline.run()
