from engine.apps.backtest.portfolio import Portfolio


class ExecutionHandler:
    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio
        pass

    def execute_orders(self):
        pass

    def _check_orders(self):
        pass

    def _check_portfolio(self):
        pass

    def _execute_order(self):
        pass

    def _update_portfolio(self):
        pass
