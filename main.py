from dotenv import load_dotenv
from binance.client import Client
from os import getenv
from API.data_fetcher import FetchData
from engine.core.bars.bars import Bars
from utils.charts.chart import Chart
import polars as pl

LEVEL_MAP = {
    "DEBUG": 10,
    "INFO": 20,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50,
}

BAR_SIZE = 100000


def pick_log_level():
    while True:
        print(LEVEL_MAP)
        log_level = int(input("Please pick a log level:").strip())
        if log_level not in LEVEL_MAP.values():
            print("Provide a valid number")
            continue
        break
    return log_level


def main():
    """Main program loop."""
    # log_level = pick_log_level()
    load_dotenv()

    chart = Chart()
    client = Client(api_key=getenv("BINANCE_API_KEY"), api_secret=getenv("BINANCE_API_SECRET"))
    data_fetcher = FetchData(client, symbol="BTCUSDT")

    bars_maker = Bars(client, data_fetcher)

    bars = bars_maker.get_dollar_bars(BAR_SIZE)

    print(bars)


if __name__ == "__main__":
    main()
