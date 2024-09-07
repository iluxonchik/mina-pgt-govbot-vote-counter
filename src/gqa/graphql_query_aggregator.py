import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, List, Dict, Optional

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
    MAX_LENGTH = 100000

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

    @staticmethod
    def _get_transactions_from_response(
        responses: list[dict],
        start_time: datetime,
        end_time: datetime,
        recent_blocks_to_ignore: int,
    ) -> List[Dict[str, Any]]:
        combined_transactions = []
        all_blocks = []

        # Collect all blocks from all responses
        for response in responses:
            all_blocks.extend(response["bestChain"])

        # Sort all blocks by their height
        all_blocks.sort(
            key=lambda b: int(b["protocolState"]["consensusState"]["blockHeight"])
        )

        # Check for block continuity
        for i in range(1, len(all_blocks)):
            prev_height = int(
                all_blocks[i - 1]["protocolState"]["consensusState"]["blockHeight"]
            )
            curr_height = int(
                all_blocks[i]["protocolState"]["consensusState"]["blockHeight"]
            )
            if curr_height != prev_height + 1:
                raise BlockDiscontinuityError(
                    f"Block height discontinuity detected: {prev_height} to {curr_height}"
                )

        # Process blocks within the time range
        for block in (
            all_blocks[:-recent_blocks_to_ignore]
            if recent_blocks_to_ignore > 0
            else all_blocks
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
                "SELECT response FROM graphql_responses WHERE min_block_timestamp <= ? AND max_block_timestamp >= ? AND endpoint = ? ORDER BY execution_timestamp",
                (
                    end_time.timestamp() * 1000,
                    start_time.timestamp() * 1000,
                    self.client.endpoint,
                ),
            )
            responses: list[dict] = cursor.fetchall()

        combined_transactions = self._get_transactions_from_response(
            responses, start_time, end_time, self.recent_blocks_to_ignore
        )

        # Log the total number of transactions retrieved
        self.logger.info(
            f"Retrieved {len(combined_transactions)} combined transactions"
        )
        return combined_transactions
