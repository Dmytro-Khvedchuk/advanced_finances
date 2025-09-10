from engine.apps.backtest.portfolio import Portfolio
from polars import pl
from engine.core.strategies.strategy import Strategy


class ExecutionHandler:
    def __init__(self, portfolio: Portfolio, strategy: Strategy):
        self.portfolio = portfolio
        self.strategy = strategy
        pass

    def process_orders(self, symbol: str, series: pl.Series):
        order = self._check_for_orders()
        if self._check_portfolio():
            self._execute_order()

    def _check_for_orders(self):
        # here should be implemented strategy calls.
        # If this function generates an order. It should be processed immidiately.

        # we create an order with status pending and type market and send it.
        self.portfolio.update_orders()
        pass

    def _check_portfolio(self):
        # This function will be for checking current metrics
        # If order can be executed and it should return True or False
        self.portfolio.get_metrics()
        pass

    def _execute_order(self):
        self.portfolio.update_positions()
