"""Vote counting module for the GovBot Vote Counter."""

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, List
from collections import defaultdict
import base58

from vote_counter.graphql_client import GraphQLClient
from vote_counter.config import Config


type VoteCount = dict[str, dict[str, Any]]


class VoteCountingPipeline:
    """Pipeline for counting votes."""

    class GraphQL:
        class Queries:
            class AllTransactionsV1:
                QUERY: str = """
                    query GetTransactions($maxLength: Int!) {
                    bestChain(maxLength: $maxLength) {
                    stateHash
                    protocolState {
                    blockchainState {
                        date
                        utcDate
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
                MAX_LENGTH: int = 100000

    REQUIRED_TX_FIELDS: List[str] = [
        "id",
        "to",
        "from",
        "amount",
        "fee",
        "memo",
        "nonce",
        "kind",
    ]

    def __init__(
        self,
        start_date: datetime,
        end_date: datetime,
        graphql_client: GraphQLClient,
        config: Config,
    ) -> None:
        self.start_date: datetime = start_date
        self.end_date: datetime = end_date
        self.client: GraphQLClient = graphql_client
        self.config: Config = config
        self.logger: logging.Logger = logging.getLogger(__name__)

        # When writing tests for the VotingPipeline, ensure that they all mock specific values
        # for self.active_query and self.active_max_length, as the .json output files under
        # tests/graphql/response/ are tied to specific values for these fields
        self.active_query: str = self.GraphQL.Queries.AllTransactionsV1.QUERY
        self.active_max_length: int = self.GraphQL.Queries.AllTransactionsV1.MAX_LENGTH

    def run(self) -> VoteCount:
        """Execute the vote counting pipeline."""
        self.logger.info(
            f"Starting vote counting pipeline from {self.start_date} to {self.end_date}"
        )

        transactions: List[dict[str, Any]] = self.get_transactions()
        filtered_transactions: List[dict[str, Any]] = self.filter_transactions(
            transactions
        )
        sequenced_transactions: List[dict[str, Any]] = self.sequence_transactions(
            filtered_transactions
        )
        vote_counts: dict[str, dict[str, Any]] = self.count_votes(
            sequenced_transactions
        )
        self.save_results(vote_counts)

        self.logger.info("Vote counting pipeline completed")

        return vote_counts

    def __ensure_required_fields(self, tx: dict[str, Any]) -> bool:
        """Ensure the transaction has all the required fields."""
        for field in self.REQUIRED_TX_FIELDS:
            if field not in tx:
                self.logger.warning(f"Missing field: {field} in transaction: {tx}")
                return False
        return True

    def get_transactions(self) -> List[dict[str, Any]]:
        """Retrieve transactions from the GraphQL API."""
        query: str = self.GraphQL.Queries.AllTransactionsV1.QUERY

        variables: dict[str, int] = {
            "maxLength": self.GraphQL.Queries.AllTransactionsV1.MAX_LENGTH
        }
        result: dict[str, Any] = self.client.execute_query(query, variables)

        transactions: List[dict[str, Any]] = []
        oldest_date: float = float("inf")
        most_recent_date: float = float("-inf")
        block_info: List[dict[str, Any]] = []

        for block_index, block in enumerate(result["bestChain"]):
            block_date: int = int(block["protocolState"]["blockchainState"]["date"])
            block_utc: datetime = datetime.fromtimestamp(
                block_date / 1000, tz=timezone.utc
            )
            block_info.append(
                {
                    "index": block_index,
                    "date": block_date,
                    "utc": block_utc,
                    "tx_count": len(block["transactions"]["userCommands"]),
                }
            )

            for tx in block["transactions"]["userCommands"]:
                if not self.__ensure_required_fields(tx):
                    continue
                tx["blockDate"] = block_date
                transactions.append(tx)

                oldest_date = min(oldest_date, block_date)
                most_recent_date = max(most_recent_date, block_date)

        # Log block information
        self.logger.info(f"Retrieved {len(block_info)} distinct blocks:")
        for block in block_info:
            self.logger.info(
                f"Block {block['index']}: "
                f"Date: {block['date']} ({block['utc'].isoformat()}), "
                f"Transactions: {block['tx_count']}"
            )

        if oldest_date == float("inf") and most_recent_date == float("-inf"):
            date_info: str = "No transactions found"
        else:
            oldest_utc: datetime = datetime.fromtimestamp(
                oldest_date / 1000, tz=timezone.utc
            )
            most_recent_utc: datetime = datetime.fromtimestamp(
                most_recent_date / 1000, tz=timezone.utc
            )
            date_info: str = (
                f"Oldest transaction date: {oldest_date} ({oldest_utc.isoformat()}), "
                f"Most recent transaction date: {most_recent_date} ({most_recent_utc.isoformat()})"
            )

        self.logger.info(f"Retrieved {len(transactions)} transactions. {date_info}")
        return transactions

    def filter_transactions(
        self, transactions: List[dict[str, Any]]
    ) -> List[dict[str, Any]]:
        """Filter transactions based on criteria."""
        filtered: List[dict[str, Any]] = []
        oldest_date: float = float("inf")
        most_recent_date: float = float("-inf")

        for tx in transactions:
            block_date: int = int(tx["blockDate"])
            utc_date: datetime = datetime.fromtimestamp(
                block_date / 1000, tz=timezone.utc
            )

            oldest_date = min(oldest_date, block_date)
            most_recent_date = max(most_recent_date, block_date)

            if "to" not in tx:
                self.logger.warning(f"Missing field: to in transaction: {tx}")
                continue

            if (
                tx["to"] == self.config.BURN_ADDRESS
                and self.start_date <= utc_date <= self.end_date
                and tx["kind"] == "PAYMENT"
                and self.is_valid_memo(tx["memo"])
            ):
                filtered.append(
                    {
                        "id": tx["id"],
                        "from": tx["from"],
                        "amount": Decimal(tx["amount"]),
                        "memo": self.decode_memo(tx["memo"]),
                        "nonce": int(tx["nonce"]),
                    }
                )

        if oldest_date == float("inf") and most_recent_date == float("-inf"):
            date_info: str = "No transactions found"
        else:
            oldest_utc: datetime = datetime.fromtimestamp(
                oldest_date / 1000, tz=timezone.utc
            )
            most_recent_utc: datetime = datetime.fromtimestamp(
                most_recent_date / 1000, tz=timezone.utc
            )
            date_info: str = (
                f"Oldest block date: {oldest_date} ({oldest_utc.isoformat()}), "
                f"Most recent block date: {most_recent_date} ({most_recent_utc.isoformat()})"
            )

        self.logger.info(
            f"Filtered down to {len(filtered)} valid vote transactions. {date_info}"
        )
        return filtered

    def sequence_transactions(
        self, transactions: List[dict[str, Any]]
    ) -> List[dict[str, Any]]:
        """Sequence transactions by nonce for each account."""
        account_transactions: dict[str, List[dict[str, Any]]] = defaultdict(list)

        for tx in transactions:
            account_transactions[tx["from"]].append(tx)

        sequenced_transactions: List[dict[str, Any]] = []
        for account, txs in account_transactions.items():
            sequenced_transactions.extend(sorted(txs, key=lambda x: x["nonce"]))

        self.logger.info(f"Sequenced {len(sequenced_transactions)} transactions")
        return sequenced_transactions

    def is_valid_memo(self, memo: str) -> bool:
        """Check if the memo is in the correct format."""
        decoded: str = self.decode_memo(memo)
        parts: List[str] = decoded.split()
        is_valid: bool = (
            len(parts) == 2 and parts[0] in ("yes", "no") and parts[1].isdigit()
        )
        self.logger.debug(f"Memo validity check: {is_valid}")
        return is_valid

    def decode_memo(self, memo: str) -> str:
        """Decode the Base58Check encoded memo."""
        self.logger.debug(f"Decoding memo: {memo}")

        try:
            # Decode from Base58Check
            decoded: bytes = base58.b58decode_check(memo)
            self.logger.debug(f"Base58Check decoded: {decoded.hex()}")

            # Check the version byte (should be 0x14)
            if decoded[0] != 0x14:
                raise ValueError(f"Invalid memo version byte: {decoded[0]}")

            # Get the length of the message (third byte) --> Docs mention byte 2, but this appears to be incorrect
            length: int = decoded[2]
            self.logger.debug(f"Memo length: {length}")

            # Extract the actual message
            message: str = decoded[3 : 3 + length].decode("utf-8")
            self.logger.debug(f"Decoded message: {message}")
            return message
        except Exception as e:
            self.logger.error(f"Error decoding memo: {str(e)}")
            return ""  # Return an empty string if decoding fails

    def count_votes(self, transactions: List[dict[str, Any]]) -> VoteCount:
        """Count votes from filtered and sequenced transactions."""
        vote_counts: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "yes_votes": {"count": 0, "addresses": set()},
                "no_votes": {"count": 0, "addresses": set()},
            }
        )

        # latest votes for each project, per account (yes or no)
        latest_votes: dict[str, dict[str, str]] = defaultdict(dict)

        for tx in transactions:
            vote, project_id_str = tx["memo"].split()
            project_id = str(
                int(project_id_str)
            )  # Convert project ID to integer and back to string
            account = tx["from"]

            # Update the latest vote for this account and project
            latest_votes[project_id][account] = vote

        # Recalculate vote counts for each project
        for project_id, votes in latest_votes.items():
            yes_votes = set(account for account, v in votes.items() if v == "yes")
            no_votes = set(account for account, v in votes.items() if v == "no")

            vote_counts[project_id] = {
                "yes_votes": {"count": len(yes_votes), "addresses": yes_votes},
                "no_votes": {"count": len(no_votes), "addresses": no_votes},
            }

        # Convert sets to lists for JSON serialization
        for project in vote_counts.values():
            project["yes_votes"]["addresses"] = list(project["yes_votes"]["addresses"])
            project["no_votes"]["addresses"] = list(project["no_votes"]["addresses"])

        self.logger.info(f"Counted votes for {len(vote_counts)} projects")
        return dict(vote_counts)

    def save_results(self, vote_counts: dict[str, dict[str, Any]]) -> None:
        """Save vote counting results to a JSON file."""
        with open("vote_counts.json", "w") as f:
            json.dump(vote_counts, f, indent=2, default=str)
        self.logger.info("Vote counts saved to vote_counts.json")
