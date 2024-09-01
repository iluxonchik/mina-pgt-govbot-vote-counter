#!/usr/bin/env python3.12
"""
GovBot Vote Counter - Main Application

This script is the entry point for the GovBot Vote Counter application.
It processes command-line arguments and orchestrates the vote counting process.
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

from vote_counter.vote_counter import VoteCountingPipeline
from vote_counter.graphql_client import GraphQLClient
from vote_counter.config import Config


def setup_logging() -> None:
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Set specific loggers to desired levels
    logging.getLogger("vote_counter").setLevel(logging.DEBUG)
    logging.getLogger("graphql_client").setLevel(logging.WARNING)
    logging.getLogger("gql.transport").setLevel(logging.WARNING)


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="GovBot Vote Counter")
    parser.add_argument(
        "start_date",
        type=str,
        help="Start date (UTC) for vote counting (YYYY-MM-DDTHH:MM:SS)",
    )
    parser.add_argument(
        "end_date",
        type=str,
        help="End date (UTC) for vote counting (YYYY-MM-DDTHH:MM:SS)",
    )
    parser.add_argument(
        "staking_ledger_epoch", type=int, help="Epoch of the staking ledger"
    )
    return parser.parse_args()


def main() -> None:
    """Main function to run the GovBot Vote Counter."""
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        args = parse_arguments()

        start_date = datetime.fromisoformat(args.start_date).replace(
            tzinfo=timezone.utc
        )
        end_date = datetime.fromisoformat(args.end_date).replace(tzinfo=timezone.utc)

        logger.info(f"Start date: {start_date}")
        logger.info(f"End date: {end_date}")
        logger.info(f"Staking ledger epoch: {args.staking_ledger_epoch}")

        config = Config()
        client = GraphQLClient(config.GRAPHQL_ENDPOINT)

        pipeline = VoteCountingPipeline(start_date, end_date, client, config)
        pipeline.run()

        logger.info("Vote counting completed successfully.")

    except Exception as e:
        logger.exception(f"An error occurred: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
