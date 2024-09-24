import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, List, Dict, Optional
import os

from vote_counter.graphql_client import GraphQLClient


class BlockDiscontinuityError(Exception):
    """Exception raised when there's a discontinuity in block heights."""

    pass


class GraphQLQueryAggregator:
    QUERY = """
    query GetTransactions($maxLength: Int!) {
      bestChain(maxLength: $maxLength) {
        stateHash
        protocolState {
          blockchainState {
            date
            utcDate
          }
          consensusState {
            blockHeight
          }
        }
        transactions {
          userCommands {
            ... on UserCommandPayment {
              id
              to
              from
              amount
              fee
              memo
              nonce
              kind
            }
          }
        }
      }
    }
    """
    MAX_LENGTH = 100000000

    def __init__(
        self,
        graphql_client: GraphQLClient,
        db_path: str,
        recent_blocks_to_ignore: int = 15,
    ):
        self.client = graphql_client
        self.db_path = db_path
        self.recent_blocks_to_ignore = recent_blocks_to_ignore
        self.logger = logging.getLogger(__name__)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS graphql_responses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_timestamp TEXT,
                    response JSON,
                    min_block_timestamp TEXT,
                    max_block_timestamp TEXT,
                    endpoint TEXT
                )
            """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_graphql_responses_execution_timestamp ON graphql_responses(execution_timestamp)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_graphql_responses_block_timestamp ON graphql_responses(min_block_timestamp, max_block_timestamp)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_graphql_responses_endpoint ON graphql_responses(endpoint)"
            )

    def retrieve_and_store(self):
        result = self.client.execute_query(self.QUERY, {"maxLength": self.MAX_LENGTH})

        execution_timestamp = datetime.now(timezone.utc).isoformat()
        min_block_timestamp = None
        max_block_timestamp = None

        for block in result["bestChain"]:
            block_timestamp = int(block["protocolState"]["blockchainState"]["date"])
            if min_block_timestamp is None or block_timestamp < min_block_timestamp:
                min_block_timestamp = block_timestamp
            if max_block_timestamp is None or block_timestamp > max_block_timestamp:
                max_block_timestamp = block_timestamp

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO graphql_responses (execution_timestamp, response, min_block_timestamp, max_block_timestamp, endpoint) VALUES (?, ?, ?, ?, ?)",
                (
                    execution_timestamp,
                    json.dumps(result),
                    min_block_timestamp,
                    max_block_timestamp,
                    self.client.endpoint,
                ),
            )

        self.logger.info(
            f"Stored GraphQL response with execution timestamp: {execution_timestamp}"
        )

    def retrieve_and_store_from_file(self, file_path: str) -> None:
        """
        Retrieve GraphQL response from a JSON file and store it in the database.

        Args:
            file_path (str): Path to the JSON file containing the GraphQL response.
        """
        self.logger.info(f"Retrieving GraphQL response from file: {file_path}")

        try:
            with open(file_path, "r") as f:
                result = json.load(f)
        except FileNotFoundError:
            self.logger.error(f"File not found: {file_path}")
            raise
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON in file: {file_path}")
            raise

        execution_timestamp = datetime.now(timezone.utc).isoformat()
        min_block_timestamp = None
        max_block_timestamp = None

        for block in result.get("bestChain", []):
            block_timestamp = int(block["protocolState"]["blockchainState"]["date"])
            if min_block_timestamp is None or block_timestamp < min_block_timestamp:
                min_block_timestamp = block_timestamp
            if max_block_timestamp is None or block_timestamp > max_block_timestamp:
                max_block_timestamp = block_timestamp

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO graphql_responses (execution_timestamp, response, min_block_timestamp, max_block_timestamp, endpoint) VALUES (?, ?, ?, ?, ?)",
                (
                    execution_timestamp,
                    json.dumps(result),
                    min_block_timestamp,
                    max_block_timestamp,
                    os.path.abspath(
                        file_path
                    ),  # Use the full path of the file as the endpoint
                ),
            )

        self.logger.info(
            f"Stored GraphQL response from file with execution timestamp: {execution_timestamp}"
        )

    @staticmethod
    def _get_transactions_from_response(
        responses: list[dict],
        start_time: datetime,
        end_time: datetime,
        recent_blocks_to_ignore: int,
        logger: logging.Logger,
    ) -> List[Dict[str, Any]]:
        combined_transactions = []
        all_blocks = {}

        # Sort responses by execution timestamp (most recent first)
        sorted_responses = sorted(
            responses, key=lambda r: r.get("execution_timestamp", ""), reverse=True
        )

        # Collect blocks from all responses, keeping only the most recent version of each block
        for response in sorted_responses:
            for block in response["bestChain"]:
                block_height = int(
                    block["protocolState"]["consensusState"]["blockHeight"]
                )
                if block_height not in all_blocks:
                    all_blocks[block_height] = block

        # Convert the dictionary to a sorted list
        sorted_blocks = sorted(
            all_blocks.values(),
            key=lambda b: int(b["protocolState"]["consensusState"]["blockHeight"]),
        )

        # Log the oldest and most recent block times and block numbers
        if sorted_blocks:
            oldest_block = sorted_blocks[0]
            newest_block = sorted_blocks[-1]

            oldest_block_time = datetime.fromtimestamp(
                int(oldest_block["protocolState"]["blockchainState"]["date"]) / 1000,
                tz=timezone.utc,
            )
            oldest_block_number = int(
                oldest_block["protocolState"]["consensusState"]["blockHeight"]
            )

            newest_block_time = datetime.fromtimestamp(
                int(newest_block["protocolState"]["blockchainState"]["date"]) / 1000,
                tz=timezone.utc,
            )
            newest_block_number = int(
                newest_block["protocolState"]["consensusState"]["blockHeight"]
            )

            logger.info(
                f"Oldest block time (UTC): {oldest_block_time.isoformat()}, Block number: {oldest_block_number}"
            )
            logger.info(
                f"Newest block time (UTC): {newest_block_time.isoformat()}, Block number: {newest_block_number}"
            )
        else:
            logger.info("No blocks found in the responses")

        # Check for block continuity
        for i in range(1, len(sorted_blocks)):
            prev_height = int(
                sorted_blocks[i - 1]["protocolState"]["consensusState"]["blockHeight"]
            )
            curr_height = int(
                sorted_blocks[i]["protocolState"]["consensusState"]["blockHeight"]
            )
            if curr_height != prev_height + 1:
                raise BlockDiscontinuityError(
                    f"Block height discontinuity detected: {prev_height} to {curr_height}"
                )

        # Process blocks within the time range
        for block in (
            sorted_blocks[:-recent_blocks_to_ignore]
            if recent_blocks_to_ignore > 0
            else sorted_blocks
        ):
            block_timestamp = int(block["protocolState"]["blockchainState"]["date"])

            if (
                start_time.timestamp() * 1000
                <= block_timestamp
                <= end_time.timestamp() * 1000
            ):
                for tx in block["transactions"]["userCommands"]:
                    tx["blockDate"] = block_timestamp
                    combined_transactions.append(tx)

        return combined_transactions

    def retrieve_combined_transactions(
        self, start_time: datetime, end_time: datetime
    ) -> List[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT response FROM graphql_responses WHERE min_block_timestamp <= ? AND max_block_timestamp >= ? ORDER BY execution_timestamp",
                (
                    end_time.timestamp() * 1000,
                    start_time.timestamp() * 1000,
                ),
            )
            responses: list[str] = cursor.fetchall()
            responses_as_dicts = [json.loads(response[0]) for response in responses]

        combined_transactions = self._get_transactions_from_response(
            responses_as_dicts,
            start_time,
            end_time,
            self.recent_blocks_to_ignore,
            self.logger,
        )

        # Log the total number of transactions retrieved
        self.logger.info(
            f"Retrieved {len(combined_transactions)} combined transactions"
        )
        return combined_transactions
