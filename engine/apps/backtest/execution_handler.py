from engine.apps.backtest.portfolio import Portfolio
from polars import pl


class ExecutionHandler:
    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio
        pass

    def process_orders(self, symbol: str, series: pl.Series):
        pass

    def _check_orders(self):
        pass

    def _check_portfolio(self):
        pass

    def _execute_order(self):
        pass

    def _update_portfolio(self):
        pass
