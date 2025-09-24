from engine.apps.backtest.portfolio import Portfolio
from engine.apps.backtest.execution_handler import ExecutionHandler
from engine.apps.backtest.report import ReportGenerator
from clickhouse_driver import Client
from utils.global_variables.GLOBAL_VARIABLES import SYMBOL
from utils.logger.logger import LoggerWrapper, log_execution
from engine.core.strategies.strategy import Strategy
from time import time

import polars as pl


class BackTest:
    def __init__(
        self,
        data: dict[str, pl.DataFrame],
        strategy: Strategy,
        log_level: int = 10,
        initial_balance: int = 10000,
        leverage: int = 1,
        maker_fee: float = 0.001,
        taker_fee: float = 0.001,
    ):
        self.logger = LoggerWrapper(name="Backtest Module", level=log_level)

        self.data = data

        self.portfolio = Portfolio(
            initial_balance=initial_balance,
            leverage=leverage,
            maker_fee=maker_fee,
            taker_fee=taker_fee,
            log_level=log_level,
        )
        self.execution_handler = ExecutionHandler(self.portfolio, strategy)
        self.report_generator = ReportGenerator(self.portfolio)

    # === User Methods ===
    @log_execution
    def run(self):
        start_time = time()
        self._iterate_through_candles()
        end_time = time()
        print(f"Backtest war running for {end_time - start_time:.3f} seconds")

    # === Helper Methods ===
    def _iterate_through_candles(self):
        df = next(iter(self.data.values()))

        open_time_values = df["open_time"].to_list()
        for timestamp in open_time_values:
            for symbol, df in self.data.items():
                series = df.filter(pl.col("open_time") == timestamp)
                self._process_orders(symbol, series)

    @log_execution
    def generate_report(self):
        self.report_generator.generate_general_metrics()
        self.report_generator.generate_symbol_metrics()

    @log_execution
    def _process_orders(self, symbol: str, series: pl.Series):
        self.execution_handler.process_orders(symbol, series)
