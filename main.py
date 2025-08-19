from dotenv import load_dotenv
from binance.client import Client
from os import getenv
from engine.apps.data_managers.market_data_manager import MarketDataManager
from engine.core.bars.bars import Bars
from utils.charts.chart import Chart

LEVEL_MAP = {
    "DEBUG": 10,
    "INFO": 20,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50,
}


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
    log_level = pick_log_level()
    load_dotenv()

    chart = Chart()
    client = Client(
        api_key=getenv("BINANCE_API_KEY"), api_secret=getenv("BINANCE_API_SECRET")
    )

    mdm = MarketDataManager(client, symbol="BTCUSDT")

    df = mdm.get_trades()

    bars_maker = Bars(log_level=log_level)

    bars, _ = bars_maker.get_tick_bars(trades_data=df, bar_size=1000)


if __name__ == "__main__":
    main()
