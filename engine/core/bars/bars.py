from binance.client import Client
from API.data_fetcher import FetchData
from engine.core.bars.dollar_bars import build_dollar_bars
from engine.core.bars.dollar_imbalance_bars import build_dollar_imbalance_bars
from engine.core.bars.time_bars import build_time_bars
from engine.core.bars.volume_bars import build_volume_bars
from engine.core.bars.tick_bars import build_tick_bars
from engine.core.bars.tick_imbalance_bars import build_tick_imbalance_bars
from engine.core.bars.volume_imbalance_bars import build_volume_imbalance_bars


class Bars:
    def __init__(self, client: Client, data_fetcher: FetchData):
        self.client = client
        self.data_fetcher = data_fetcher

    def _get_trades_data(self):
        #TODO: this should be somehow modified to be more flexible
        return self.data_fetcher.fetch_recent_trades(limit=1000)

    def _get_klines_data(self):
        # TODO: this should be somehow modified to be more flexible
        return self.data_fetcher.fetch_klines()

    def get_tick_bars(self, bar_size: int = 10):
        trades_data = self._get_trades_data()
        return build_tick_bars(trades_data, bar_size=bar_size)

    def get_volume_bars(self, bar_size: float = 1):
        trades_data = self._get_trades_data()
        return build_volume_bars(trades_data, bar_size=bar_size)

    def get_dollar_bars(self, bar_size: float = 1):
        trades_data = self._get_trades_data()
        return build_dollar_bars(trades_data, bar_size=bar_size)

    def get_kline_bars(self):
        klines_data = self._get_klines_data()
        return build_time_bars(klines_data)

    def get_tick_imbalance_bars(self):
        trades_data = self._get_trades_data()
        return build_tick_imbalance_bars(trades_data)

    def get_volume_imbalance_bars(self):
        trades_data = self._get_trades_data()
        return build_volume_imbalance_bars(trades_data)

    def get_dollar_imbalance_bars(self):
        trades_data = self._get_trades_data()
        return build_dollar_imbalance_bars(trades_data)
