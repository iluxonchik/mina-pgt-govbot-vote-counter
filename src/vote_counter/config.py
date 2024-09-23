"""Configuration module for the GovBot Vote Counter."""


class Config:
    """Configuration class for the application."""

    def __init__(self):
        self.GRAPHQL_ENDPOINT = "https://devnet.minaprotocol.network/graphql"
        self.BURN_ADDRESS = "B62qiburnzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzmp7r7UN6X"
        self.DB_PATH = "govbot_gqa.db"
        self.OUTPUT_FILE = "vote_counts.json"
        self.STAKE_OUTPUT_FILE = "vote_stake_info.json"
        self.RECENT_BLOCKS_TO_IGNORE = 15

    # Add more configuration constants as needed
