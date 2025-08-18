class TradeEvaluation:
    # here should be implemented client and real prices
    def __init__(self, balance: float):
        self.balance = balance
        self.balance_history = []
        self.trade_history = []
        self.last_order_price = 0
        self.buy_amount = 0.1
        self.sell_amount = 0.1
        self.is_in_trade = False
        self.position = None
        self.commission = 0.0007

    def update_balance_history(self):
        self.balance_history.append(self.balance)

    def update_trade_history(self, trade_profit: float):
        self.trade_history.append(trade_profit)

    # should be api implementation WITH MANDATORY CHECK IF SUCCESSFUL AND IF PRICE IS RIGHT
    def process_signal(self, signal: str, best_price: float):
        if signal == "BUY" and not self.is_in_trade:
            self.buy(best_price)
        elif signal == "SELL" and not self.is_in_trade:
            self.sell(best_price)
        elif signal == "OVER" and self.is_in_trade:
            self.stop(best_price)

    def stop(self, best_price: float):
        if self.position == "BUY":
            commission = best_price * self.commission * self.buy_amount
            print("IN BUY FUNCTION BEST PRICE IS: ", best_price)
            profit = (
                best_price * self.buy_amount
                - self.last_order_price * self.buy_amount
                - commission
            )
        elif self.position == "SELL":
            commission = best_price * self.commission * self.sell_amount
            print("IN SELL FUNCTION BEST PRICE IS: ", best_price)
            profit = (
                self.last_order_price * self.sell_amount
                - best_price * self.sell_amount
                - commission
            )
        self.balance += profit
        print("Profit: ", profit)
        self.balance_history.append(self.balance)
        self.trade_history.append(profit)
        self.last_order_price = best_price
        self.is_in_trade = False
        self.position = None

    def buy(self, best_price: float):
        self.last_order_price = best_price
        self.position = "BUY"
        self.is_in_trade = True

    def sell(self, best_price: float):
        self.last_order_price = best_price
        self.position = "SELL"
        self.is_in_trade = True
