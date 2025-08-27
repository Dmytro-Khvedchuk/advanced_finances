from binance.client import Client as BinanceClient
from dotenv import load_dotenv
from engine.apps.data_managers.clickhouse.client import get_clickhouse_client
from engine.apps.data_managers.market_data_manager import MarketDataManager
from engine.core.bars.bars import Bars
from os import getenv
from utils.charts.chart import Chart
from utils.global_variables.GLOBAL_VARIABLES import LEVEL_MAP, SYMBOL


def pick_log_level():
    while True:
        print(LEVEL_MAP)
        log_level = int(input("Please pick a log level: ").strip())
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
    binance_client = BinanceClient(
        api_key=getenv("BINANCE_API_KEY"), api_secret=getenv("BINANCE_API_SECRET")
    )

    database_client = get_clickhouse_client()

    mdm = MarketDataManager(
        binance_client=binance_client,
        database_client=database_client,
        symbol=SYMBOL,
        log_level=log_level,
    )

    data = mdm.trade_manager.get_trades(start_id=5_190_000_000, end_id=5_190_100_000)

    print(data)


if __name__ == "__main__":
    main()
