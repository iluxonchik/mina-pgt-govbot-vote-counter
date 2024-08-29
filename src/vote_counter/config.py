"""Configuration module for the GovBot Vote Counter."""


class Config:
    """Configuration class for the application."""

    def __init__(self):
        self.GRAPHQL_ENDPOINT = "https://api.minascan.io/node/devnet/v1/graphql"
        self.BURN_ADDRESS = "B62qiburnzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzmp7r7UN6X"

    # Add more configuration constants as needed
