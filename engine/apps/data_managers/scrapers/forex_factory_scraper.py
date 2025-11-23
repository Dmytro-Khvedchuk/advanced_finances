from utils.logger.logger import LoggerWrapper
from bs4 import BeautifulSoup
from utils.global_variables.GLOBAL_VARIABLES import MONTHS
import cloudscraper


class ForexFactoryScraper:
    def __init__(self, log_level: int = 10):
        self.logger = LoggerWrapper(
            name="Forex Factory Scraper Module", level=log_level
        )

        self.base_url = "https://www.forexfactory.com/"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/117.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }

        self.scraper = cloudscraper.create_scraper()

    def get_data(self, from_month: int, from_year: int, duration_months: int):
        self.run_through_months(
            from_month=from_month,
            from_year=from_year,
            duration_months=duration_months
        )
        # response = self.scraper.get(self.url)
        # soup = BeautifulSoup(response.text, "lxml")

    def run_through_months(self, from_month: int, duration_months: int, from_year: int):
        month = from_month
        year = from_year

        for i in range(duration_months):
            print(f"month {i}")
            print(f"date: {MONTHS[month]}, {year}")

            month += 1
            if month > 12:
                month = 1
                year += 1