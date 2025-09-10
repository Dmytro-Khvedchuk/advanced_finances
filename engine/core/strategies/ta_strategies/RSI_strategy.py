from engine.core.strategies.strategy import Strategy

class RSIStrategy(Strategy):
    def generate_orders(self, market_data: dict[str, dict]):
        # this function will generate orders
        pass

    def get_required_data(self):
        # this function will return the amount of the
        # required data for creating a signal, this one needs only 2 curr and prev
        pass