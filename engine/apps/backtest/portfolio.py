from utils.logger.logger import LoggerWrapper, log_execution
import polars as pl
from utils.global_variables.SCHEMAS import (
    TRADE_HISTORY_SCHEMA,
    ORDER_HISTORY_SCHEMA,
    POSITIONS_SCHEMA,
)

from collections import defaultdict


class Portfolio:
    def __init__(self, initial_balance, leverage, maker_fee, taker_fee, log_level):
        self.logger = LoggerWrapper(name="Portfolio Module", level=log_level)

        self.trade_history = pl.DataFrame(schema=TRADE_HISTORY_SCHEMA, orient="row")
        self.order_history = pl.DataFrame(schema=ORDER_HISTORY_SCHEMA, orient="row")
        self.current_positions = pl.DataFrame(schema=POSITIONS_SCHEMA, orient="row")
        self.order_id = 0
        self.equity = initial_balance
        self.equity_history = defaultdict(dict)

        self.leverage = leverage
        self.initial_capital = initial_balance
        self.maker_fee = maker_fee
        self.taker_fee = taker_fee

    @log_execution
    def get_metrics(self):
        return (
            self.equity_history,
            self.trade_history,
            self.order_history,
            self.current_positions,
            self.initial_capital,
        )

    @log_execution
    def update_orders(self, order):
        order = pl.DataFrame(schema=ORDER_HISTORY_SCHEMA, data=order)
        order = order.with_columns(pl.lit(self.order_id).alias("order_id"))
        self.order_id += 1
        order = order.cast(self.order_history.schema)
        self.order_history = pl.concat([self.order_history, order])

    @log_execution
    def update_positions(self, symbol, series):
        orders_to_be_executed = self.order_history.filter(
            (pl.col("status") == "PENDING") & (pl.col("symbol") == symbol)
        )
        if not orders_to_be_executed.is_empty():
            self._execute_orders(orders_to_be_executed, series)

        self._update_positions_stats(symbol=symbol, series=series)

    @log_execution
    def _execute_orders(self, orders, series):
        for order in orders.to_dicts():
            position = {
                "order_id": order["order_id"],
                "symbol": order["symbol"],
                "volume": order["volume"] * self.leverage,
                "direction": order["direction"],
                "entry_time": series["open_time"],
                "entry_price": series["close"],
                "leverage": self.leverage,
                "strategy": order["strategy"],
                "unrealized_pnl": 0,
                "realized_pnl": 0,
                "take_profit": order["take_profit"],
                "stop_loss": order["stop_loss"],
            }
            if position["volume"] > self.equity:
                # This should be logged print("not enough money!")
                continue
            self.equity -= position["volume"] / self.leverage

            new_position = pl.DataFrame(position).cast(self.current_positions.schema)
            self.current_positions = pl.concat([self.current_positions, new_position])

            self.order_history = self.order_history.with_columns(
                pl.when(pl.col("order_id") == order["order_id"])
                .then(pl.lit("FILLED"))
                .otherwise(pl.col("status"))
                .alias("status")
            )

    @log_execution
    def _record_trade(self, position, closed_by, timestamp):
        self.current_positions = self.current_positions.filter(
            pl.col("order_id") != position["order_id"]
        )

        pnl = 0
        if closed_by == "TP":
            pnl = self._calculate_pnl(
                entry_price=position["entry_price"],
                current_price=position["take_profit"],
                volume=position["volume"],
                direction=position["direction"],
            )
        elif closed_by == "SL":
            pnl = self._calculate_pnl(
                entry_price=position["entry_price"],
                current_price=position["stop_loss"],
                volume=position["volume"],
                direction=position["direction"],
            )

        commissions = (
            position["volume"] * self.maker_fee + position["volume"] * self.taker_fee
        )

        trade = {
            "order_id": position["order_id"],
            "symbol": position["symbol"],
            "pnl": pnl,
            "volume": position["volume"],
            "direction": position["direction"],
            "entry_price": position["entry_price"],
            "entry_time": position["entry_time"],
            "exit_time": timestamp,
            "strategy": position["strategy"],
            "stop_loss": position["stop_loss"],
            "break_even": 0,
            "take_profit": position["take_profit"],
            "closed_by": closed_by,
            "commissions": commissions,
        }

        trade_df = pl.DataFrame(schema=TRADE_HISTORY_SCHEMA, data=trade, orient="row")

        self.equity += (position["volume"] / self.leverage) + pnl - commissions

        self.trade_history = pl.concat([self.trade_history, trade_df])

    @log_execution
    def _update_positions_stats(self, symbol, series):
        high = series["high"][-1]
        low = series["low"][-1]
        close = series["close"][-1]
        timestamp = series["open_time"][-1]

        positions_by_symbol = self.current_positions.filter(pl.col("symbol") == symbol)

        for position in positions_by_symbol.to_dicts():
            if position["direction"] == "BUY" and symbol == position["symbol"]:
                if high > position["take_profit"]:
                    closed_by = "TP"
                    self._record_trade(
                        position=position, closed_by=closed_by, timestamp=timestamp
                    )

                elif low < position["stop_loss"]:
                    closed_by = "SL"
                    self._record_trade(
                        position=position, closed_by=closed_by, timestamp=timestamp
                    )
                else:
                    self.current_positions = self.current_positions.filter(
                        pl.col("order_id") != position["order_id"]
                    )
                    position["unrealized_pnl"] = self._calculate_pnl(
                        entry_price=position["entry_price"],
                        current_price=close,
                        volume=position["volume"],
                        direction=position["direction"],
                    )
                    self.current_positions = pl.concat(
                        [
                            self.current_positions,
                            pl.DataFrame(data=position, schema=POSITIONS_SCHEMA),
                        ]
                    )

            else:
                if low < position["take_profit"]:
                    closed_by = "TP"
                    self._record_trade(
                        position=position, closed_by=closed_by, timestamp=timestamp
                    )

                elif high > position["stop_loss"]:
                    closed_by = "SL"
                    self._record_trade(
                        position=position, closed_by=closed_by, timestamp=timestamp
                    )

                else:
                    self.current_positions = self.current_positions.filter(
                        pl.col("order_id") != position["order_id"]
                    )
                    position["unrealized_pnl"] = self._calculate_pnl(
                        entry_price=position["entry_price"],
                        current_price=close,
                        volume=position["volume"],
                        direction=position["direction"],
                    )

                    self.current_positions = pl.concat(
                        [
                            self.current_positions,
                            pl.DataFrame(data=position, schema=POSITIONS_SCHEMA),
                        ]
                    )

        current_equity = self.equity
        unrealized_pnl = self.current_positions["unrealized_pnl"].sum()
        volume_in_positions = self.current_positions["volume"].sum() / self.leverage
        commissions = self.trade_history["commissions"].sum()
        total = current_equity + unrealized_pnl + volume_in_positions - commissions

        symbol_pnl = self._calculate_symbol_pnl(symbol=symbol)

        self.equity_history[symbol].update({timestamp: symbol_pnl})
        self.equity_history["General"].update({timestamp: total})

    @log_execution
    def _calculate_symbol_pnl(self, symbol: str):
        symbol_data = self.current_positions.filter(pl.col("symbol") == symbol)
        realized_total_pnl = self.trade_history.filter(pl.col("symbol") == symbol)[
            "pnl"
        ].sum()
        unrealized_position_pnl = symbol_data["unrealized_pnl"].sum()
        realized_position_pnl = symbol_data["realized_pnl"].sum()
        return realized_position_pnl + unrealized_position_pnl + realized_total_pnl

    @log_execution
    @staticmethod
    def _calculate_pnl(
        entry_price: float, current_price: float, volume: float, direction: str
    ) -> float:

        asset_volume = volume / entry_price

        if direction == "BUY":
            return (current_price - entry_price) * asset_volume
        elif direction == "SELL":
            return (entry_price - current_price) * asset_volume
        else:
            raise ValueError(f"Invalid direction: {direction}")
