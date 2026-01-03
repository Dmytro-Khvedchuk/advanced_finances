"""The start of the program."""
from datetime import datetime, UTC

import polars as pl
from dotenv import load_dotenv

from src.app.data.api.domain.value_objects import BinanceKlineTimeframes
from src.app.data.api.market import MarketData
from src.app.data.api.scrapers import ForexFactoryScraper
from src.app.system.logs import setup_logging


def main() -> None:
    """Main program loop."""
    setup_logging() 
    load_dotenv()

    # news fetcher
    ff_scraper: ForexFactoryScraper = ForexFactoryScraper()
    forex_news: pl.DataFrame = ff_scraper.get_data(1, 2023, 1)  # noqa: F841

    # klines fetcher
    market_data: MarketData = MarketData()
    klines: pl.DataFrame = market_data.get_klines(  # noqa: F841
        datetime(2025, 12, 2, tzinfo=UTC), 
        datetime(2026, 1, 2, tzinfo=UTC), 
        "BTCUSDT", 
        BinanceKlineTimeframes.FIVE_MINUTES
    )


if __name__ == "__main__":
    main()
