"""Binance client factory for authenticated API access."""

from binance.client import Client  # type: ignore[reportMissingTypeStubs]

from src.app.system.logs import get_logger
from src.app.system.secrets import Secrets


logger = get_logger(__name__)


def get_binance_client() -> Client:
    """Create an authenticated Binance client using configured secrets.
    
    Returns:
        Client: Binance client for the klines fetching.
    """
    logger.info("Initializing binance client.")
    binance_client: Client = Client(
        api_key=Secrets.binance_api_key, 
        api_secret=Secrets.binance_secret_key
    )
    logger.info("Initialized binance client successfully.")
    return binance_client
