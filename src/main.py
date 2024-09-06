#!/usr/bin/env python3.12
"""
GovBot Vote Counter - Main Application

This script is the entry point for the GovBot Vote Counter application.
It supports two modes: Query Aggregator and Vote Counting.
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

from vote_counter.vote_counter import VoteCountingPipeline
from vote_counter.graphql_client import GraphQLClient
from vote_counter.config import Config
from gqa.graphql_query_aggregator import GraphQLQueryAggregator, BlockDiscontinuityError


def setup_logging(verbose: bool = False) -> None:
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("graphql_client").setLevel(logging.WARNING)
    logging.getLogger("gql.transport").setLevel(logging.WARNING)


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="GovBot Vote Counter",
        epilog="Use 'govbot-vote-counter <command> --help' for more information on a command.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Query Aggregator Mode
    aggregate_parser = subparsers.add_parser(
        "aggregate", help="Retrieve and store GraphQL query results"
    )

    # Vote Counting Mode
    count_parser = subparsers.add_parser("count", help="Count votes from stored data")
    count_parser.add_argument(
        "start_date",
        type=str,
        help="Start date (UTC) for vote counting (YYYY-MM-DDTHH:MM:SS)",
    )
    count_parser.add_argument(
        "end_date",
        type=str,
        help="End date (UTC) for vote counting (YYYY-MM-DDTHH:MM:SS)",
    )
    count_parser.add_argument(
        "--output", help="Output file for vote counts (overrides config)"
    )

    return parser.parse_args()


def run_query_aggregator(config: Config) -> None:
    """Run the Query Aggregator mode."""
    logger = logging.getLogger(__name__)
    client = GraphQLClient(config.GRAPHQL_ENDPOINT)
    gqa = GraphQLQueryAggregator(client, config.DB_PATH)

    logger.info(f"Retrieving and storing data from {config.GRAPHQL_ENDPOINT}")
    gqa.retrieve_and_store()
    logger.info("Data retrieval and storage completed successfully")


def run_vote_counting(args: argparse.Namespace, config: Config) -> None:
    """Run the Vote Counting mode."""
    logger = logging.getLogger(__name__)

    start_date = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
    end_date = datetime.fromisoformat(args.end_date).replace(tzinfo=timezone.utc)

    logger.info(f"Start date: {start_date}")
    logger.info(f"End date: {end_date}")

    client = GraphQLClient(config.GRAPHQL_ENDPOINT)
    gqa = GraphQLQueryAggregator(client, config.DB_PATH)
    pipeline = VoteCountingPipeline(start_date, end_date, gqa, config)

    try:
        vote_counts = pipeline.run()
        output_file = args.output or config.OUTPUT_FILE
        pipeline.save_results(vote_counts, output_file)
        logger.info(
            f"Vote counting completed successfully. Results saved to {output_file}"
        )
    except BlockDiscontinuityError as e:
        logger.error(f"Vote counting failed due to block discontinuity: {str(e)}")
        sys.exit(1)


def main() -> None:
    """Main function to run the GovBot Vote Counter."""
    args = parse_arguments()
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    try:
        config = Config()

        if args.command == "aggregate":
            run_query_aggregator(config)
        elif args.command == "count":
            run_vote_counting(args, config)

    except Exception as e:
        logger.exception(f"An error occurred: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
