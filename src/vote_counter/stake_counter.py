"""Stake counting module for the GovBot Vote Counter."""

import json
import logging
from decimal import Decimal
from typing import Any, Dict

from vote_counter.graphql_client import GraphQLClient
from vote_counter.config import Config


class StakeCountingPipeline:
    """Pipeline for counting vote stakes."""

    def __init__(self, graphql_client: GraphQLClient, config: Config):
        self.client = graphql_client
        self.config = config
        self.logger = logging.getLogger(__name__)

    def run(self, input_file: str, output_file: str) -> Dict[str, Any]:
        """Execute the stake counting pipeline."""
        self.logger.info(f"Starting stake counting pipeline")

        vote_counts = self.load_vote_counts(input_file)
        stake_info = self.count_stakes(vote_counts)
        self.save_results(stake_info, output_file)

        self.logger.info("Stake counting pipeline completed")
        return stake_info

    def load_vote_counts(self, input_file: str) -> Dict[str, Any]:
        """Load vote counts from the input file."""
        with open(input_file, "r") as f:
            return json.load(f)

    def count_stakes(self, vote_counts: Dict[str, Any]) -> Dict[str, Any]:
        """Count stakes for each vote."""
        stake_info = {}
        total_supply = self.get_total_supply()

        for project_id, votes in vote_counts.items():
            stake_info[project_id] = {
                "yes_votes": self.get_stake_info(votes["yes_votes"], total_supply),
                "no_votes": self.get_stake_info(votes["no_votes"], total_supply),
            }

        return stake_info

    def get_stake_info(
        self, votes: Dict[str, Any], total_supply: Decimal
    ) -> Dict[str, Any]:
        """Get stake information for a set of votes."""
        addresses = votes["addresses"]
        stake_info = {
            "count": votes["count"],
            "addresses": addresses,
            "stake": {"addresses": {}, "total": Decimal(0), "percent": Decimal(0)},
        }

        for address in addresses:
            balance = self.get_account_balance(address)
            percent = (balance / total_supply) * 100
            stake_info["stake"]["addresses"][address] = {
                "balance": balance,
                "percent": percent,
            }
            stake_info["stake"]["total"] += balance

        stake_info["stake"]["percent"] = (
            stake_info["stake"]["total"] / total_supply
        ) * 100
        return stake_info

    def get_account_balance(self, address: str) -> Decimal:
        """Get the balance of an account using GraphQL query."""
        query = """
        query StakingInfo($publicKey: PublicKey!) {
          account(publicKey: $publicKey) {
            balance {
              total
            }
          }
        }
        """
        variables = {"publicKey": address}
        result = self.client.execute_query(query, variables)
        return Decimal(result["account"]["balance"]["total"])

    def get_total_supply(self) -> Decimal:
        """Get the total circulating currency using GraphQL query."""
        query = """
        query getTotalCurrency {
          bestChain(maxLength: 1) {
            protocolState {
              consensusState {
                totalCurrency
              }
            }
          }
        }
        """
        result = self.client.execute_query(query, {})
        return Decimal(
            result["bestChain"][0]["protocolState"]["consensusState"]["totalCurrency"]
        )

    def save_results(self, stake_info: Dict[str, Any], output_file: str) -> None:
        """Save stake counting results to a JSON file."""
        with open(output_file, "w") as f:
            json.dump(stake_info, f, indent=2, default=str)
        self.logger.info(f"Stake information saved to {output_file}")
