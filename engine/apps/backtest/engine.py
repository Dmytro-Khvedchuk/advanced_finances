from engine.apps.backtest.portfolio import Portfolio
from engine.apps.backtest.execution_handler import ExecutionHandler
from engine.apps.backtest.report import ReportGenerator
from clickhouse_driver import Client
from utils.global_variables.GLOBAL_VARIABLES import SYMBOL
from utils.logger.logger import LoggerWrapper, log_execution


class BackTest:
    def __init__(
        self,
        data: dict,
        log_level: int = 10,
        initial_balance: int = 10000,
        maker_fee: float = 0.001,
        taker_fee: float = 0.001,
    ):
        self.logger = LoggerWrapper(name="Backtest Module", level=log_level)
        
        # data will be stored as {symbol : data}
        self.data = data

        self.portfolio = Portfolio(
            initial_balance=initial_balance,
            maker_fee=maker_fee,
            taker_fee=taker_fee,
            log_level=log_level,
        )
        self.execution_handler = ExecutionHandler(self.portfolio)
        self.report_generator = ReportGenerator(self.portfolio)

    # === User Methods ===
    @log_execution
    def run(self):
        self._update_current_candle()
        self._execute_orders()

    @log_execution
    def generate_report(self):
        # This should be either pdf report, or just metrics in console
        pass

    # === Helper Methods ===
    def _update_current_candle(self):
        pass

    @log_execution
    def _execute_orders(self):
        self.execution_handler.execute_orders()
