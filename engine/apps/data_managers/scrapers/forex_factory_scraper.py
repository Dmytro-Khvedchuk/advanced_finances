from utils.logger.logger import LoggerWrapper
from bs4 import BeautifulSoup

import cloudscraper


class ForexFactoryScraper:
    def __init__(self, log_level: int = 10):
        self.logger = LoggerWrapper(
            name="Forex Factory Scraper Module", level=log_level
        )

        self.url = "https://www.forexfactory.com/"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/117.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }

        self.scraper = cloudscraper.create_scraper()

    def get_data(self):
        response = self.scraper.get(self.url)
        soup = BeautifulSoup(response.text, "lxml")
