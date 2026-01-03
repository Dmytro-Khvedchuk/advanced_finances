"""The start of the program."""
import polars as pl
from dotenv import load_dotenv

from src.app.data.api.scrapers import ForexFactoryScraper
from src.app.system.logs import setup_logging


def main() -> None:
    """Main program loop."""
    setup_logging() 
    load_dotenv()

    # news fetcher
    ff_scraper: ForexFactoryScraper = ForexFactoryScraper()
    forex_news: pl.DataFrame = ff_scraper.get_data(1, 2023, 24)  # noqa: F841


if __name__ == "__main__":
    main()
