from binance.client import Client as BinanceClient
from dotenv import load_dotenv
from engine.apps.backtest.engine import BackTest
from engine.apps.data_managers.market_data_manager import MarketDataManager
from engine.apps.data_managers.clickhouse.client import get_clickhouse_client
from os import getenv
from utils.charts.chart import Chart
from utils.global_variables.GLOBAL_VARIABLES import LEVEL_MAP, SYMBOL

from engine.core.strategies.ta_strategies.RSI_strategy import RSIStrategy


def pick_log_level():
    """Function for picking log level"""
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

    symbols = [
        "BTCUSDT",
        "ETHUSDT",
        "LINKUSDT",
        "SOLUSDT",
        "XRPUSDT",
        "SUIUSDT",
        "AVAXUSDT",
    ]
    timeframe = "1h"
    start_date = "Aug 10 2025"
    end_date = "Aug 30 2025"
    initial_balance = 10000.0
    leverage = 4
    maker_fee = 0.0002
    taker_fee = 0.0004
    strategy = RSIStrategy(log_level=log_level)

    data = {}

    for symbol in symbols:
        mdm.update_symbol(symbol)
        klines = mdm.kline_manager.get_klines(
            start_date=start_date, end_date=end_date, timeframe=timeframe
        )
        data.update({symbol: klines})

    backtest_engine = BackTest(
        data=data,
        strategy=strategy,
        log_level=log_level,
        initial_balance=initial_balance,
        leverage=leverage,
        maker_fee=maker_fee,
        taker_fee=taker_fee,
    )

    backtest_engine.run()

    backtest_engine.generate_report(pdf=True, file_name="RSI Strategy Report.pdf")


if __name__ == "__main__":
    main()
