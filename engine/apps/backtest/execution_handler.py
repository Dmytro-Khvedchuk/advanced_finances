from engine.apps.backtest.portfolio import Portfolio
import polars as pl
from engine.core.strategies.strategy import Strategy


class ExecutionHandler:
    def __init__(self, portfolio: Portfolio, strategy: Strategy):
        self.portfolio = portfolio
        self.strategy = strategy
        pass

    def process_orders(self, symbol: str, series: pl.Series):
        order = self._check_for_orders(symbol=symbol, series=series)
        if order is not None:
            self.portfolio.update_orders(order=order)
        self.portfolio.update_positions(symbol=symbol, series=series)

    def _check_for_orders(self, symbol: str, series: pl.Series):
        order = self.strategy.generate_order(symbol=symbol, new_series=series)
        if order is None:
            return None
        return order

    def _check_portfolio(self):
        # This function will be for checking current metrics
        # If order can be executed and it should return True or False
        self.portfolio.get_metrics()
        pass
