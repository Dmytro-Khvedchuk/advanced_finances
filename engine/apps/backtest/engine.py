from engine.apps.backtest.execution_handler import ExecutionHandler
from engine.apps.backtest.portfolio import Portfolio
from engine.apps.backtest.report import ReportGenerator
from engine.core.strategies.strategy import Strategy
from polars import DataFrame, Series, col
from time import time
from utils.logger.logger import LoggerWrapper, log_execution


class BackTest:
    def __init__(
        self,
        data: dict[str, DataFrame],
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
        self.execution_handler = ExecutionHandler(
            portfolio=self.portfolio, strategy=strategy, log_level=log_level
        )
        self.report_generator = ReportGenerator(self.portfolio, log_level=log_level)
        self.strategy_name = strategy.__class__.__name__

    @log_execution
    def run(self):
        start_time = time()
        self._iterate_through_candles()
        end_time = time()
        print(f"Backtest war running for {end_time - start_time:.3f} seconds")

    @log_execution
    def _iterate_through_candles(self):
        df = next(iter(self.data.values()))

        open_time_values = df["open_time"].to_list()
        for timestamp in open_time_values:
            for symbol, df in self.data.items():
                series = df.filter(col("open_time") == timestamp)
                self._process_orders(symbol, series)

    @log_execution
    def generate_report(
        self, pdf: bool = False, file_name: str = "strategy_report.pdf"
    ):
        self.report_generator.generate_general_metrics()
        self.report_generator.generate_symbol_metrics()
        if pdf:
            self.report_generator.generate_pdf_report(
                strategy_name=self.strategy_name, output_file_path=file_name
            )

    @log_execution
    def _process_orders(self, symbol: str, series: Series):
        self.execution_handler.process_orders(symbol, series)
