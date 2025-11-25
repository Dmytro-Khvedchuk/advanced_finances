from utils.logger.logger import LoggerWrapper
from engine.apps.data_managers.scrapers.forex_factory_scraper import ForexFactoryScraper


class ScraperManager:
    def __init__(self, log_level: int = 10):
        self.logger = LoggerWrapper(name="Scraper Manager Module", level=log_level)

        self.forex_factory_manager = ForexFactoryScraper(log_level=log_level)
