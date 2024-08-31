"""GraphQL client module for interacting with the Mina blockchain."""

import json
import logging
from typing import Any, Dict
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport


class GraphQLClient:
    """GraphQL client for querying the Mina blockchain."""

    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        self.logger = logging.getLogger(__name__)
        transport = RequestsHTTPTransport(url=self.endpoint)
        self.client = Client(transport=transport, fetch_schema_from_transport=True)

    def execute_query(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a GraphQL query.

        Args:
            query (str): The GraphQL query string.
            variables (Dict[str, Any]): Variables for the query.

        Returns:
            Dict[str, Any]: The query result.
        """
        self.logger.info(f"Executing GraphQL query: {query[:50]}...")
        self.logger.debug(f"Query variables: {variables}")
        try:
            result = self.client.execute(gql(query), variable_values=variables)
            self.logger.debug(f"Query result: {result}")
            return result
        except Exception as e:
            self.logger.exception(f"Error executing GraphQL query: {str(e)}")
            raise
