from utils.logger.logger import LoggerWrapper, log_execution


class Portfolio:
    def __init__(self, initial_balance, leverage, maker_fee, taker_fee, log_level):
        # variables
        self.balance_history = []
        self.trade_history = {}
        self.order_history = {}
        self.current_positions = {}
        self.order_id = 0
        self.equity = initial_balance

        # parameters
        self.leverage = leverage
        self.initial_capital = initial_balance
        self.maker_fee = maker_fee
        self.taker_fee = taker_fee

        self.logger = LoggerWrapper(name="Portfolio Module", level=log_level)

    # Here should be method that will allow or disallow for orders
    # Also it should have something like liquidation chech for each candles

    def get_metrics(self):
        pass
