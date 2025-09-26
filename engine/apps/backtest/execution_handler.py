from engine.apps.backtest.portfolio import Portfolio
from engine.core.strategies.strategy import Strategy
from polars import Series
from utils.logger.logger import LoggerWrapper


class ExecutionHandler:
    def __init__(self, portfolio: Portfolio, strategy: Strategy, log_level: int = 10):
        self.logger = LoggerWrapper(name="Execution Handler Module", level=log_level)
        self.portfolio = portfolio
        self.strategy = strategy
        pass

    def process_orders(self, symbol: str, series: Series):
        order = self._check_for_orders(symbol=symbol, series=series)
        if order is not None:
            self.portfolio.update_orders(order=order)
        self.portfolio.update_positions(symbol=symbol, series=series)

    def _check_for_orders(self, symbol: str, series: Series):
        order = self.strategy.generate_order(symbol=symbol, new_series=series)
        if order is None:
            return None
        return order

    def _check_portfolio(self):
        self.portfolio.get_metrics()
        pass
