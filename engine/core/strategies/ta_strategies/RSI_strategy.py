from engine.core.strategies.strategy import Strategy
import polars as pl
import ta
from utils.global_variables.SCHEMAS import ORDER_HISTORY_SCHEMA


class RSIStrategy(Strategy):
    def __init__(self, rsi_period: int = 14, move: float = 0.05):
        self.candles_for_indicators = rsi_period
        self.candles_for_signal = 1
        self.data = {}
        self.move = move
        self.strategy_name = "RSI Continuation Strategy"

    def generate_order(self, symbol: str, new_series: pl.Series):
        self._update_data(symbol=symbol, new_series=new_series)
        order = self._process_signal(symbol)
        return order

    def _process_signal(self, symbol: str):
        data = self.data[symbol].tail(self.candles_for_signal)
        order = None
        if "rsi" not in data.columns:
            return order
        last_rsi = data["rsi"][-1]
        if last_rsi > 85.0:
            take_profit = data["close"] + data["close"] * self.move
            stop_loss = data["close"] - data["close"] * self.move
            order_info = {
                "order_id": None,
                "symbol": symbol,
                "volume": 200,
                "direction": "BUY",
                "order_type": "MARKET",
                "order_time": data["open_time"],
                "strategy": self.strategy_name,
                "status": "PENDING",
                "entry_price": data["close"][-1],
                "take_profit": take_profit,
                "stop_loss": stop_loss,
            }
            order = pl.DataFrame(order_info, schema=ORDER_HISTORY_SCHEMA)

        if last_rsi < 15.0:
            take_profit = data["close"] - data["close"] * self.move
            stop_loss = data["close"] + data["close"] * self.move
            order_info = {
                "order_id": None,
                "symbol": symbol,
                "volume": 200,
                "direction": "SELL",
                "order_type": "MARKET",
                "order_time": data["open_time"],
                "strategy": self.strategy_name,
                "status": "PENDING",
                "entry_price": data["close"][-1],
                "take_profit": take_profit,
                "stop_loss": stop_loss,
            }
            order = pl.DataFrame(order_info, schema=ORDER_HISTORY_SCHEMA)

        return order

    def _update_data(self, symbol: str, new_series: pl.Series):
        if symbol not in self.data.keys():
            new_df = pl.DataFrame(new_series)
            self.data.update({symbol: new_df})
        else:
            df = self.data[symbol]
            df = df.tail(self.candles_for_signal + self.candles_for_indicators - 1)
            if "rsi" in df.columns:
                df = df.drop("rsi")
            new_df = pl.concat([df, new_series])
            if new_df.height == self.candles_for_signal + self.candles_for_indicators:
                new_df = self._calculate_rsi(symbol, new_df)
            self.data.update({symbol: new_df})

    def _calculate_rsi(self, symbol: str, df: pl.DataFrame):
        close_pd = df["close"].to_pandas()
        rsi = ta.momentum.RSIIndicator(
            close=close_pd, window=self.candles_for_indicators
        ).rsi()
        return df.with_columns(pl.Series("rsi", rsi.values))
