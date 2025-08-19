import polars as pl
from engine.core.bars.dollar_bars import build_dollar_bars
from engine.core.bars.dollar_imbalance_bars import build_dollar_imbalance_bars
from engine.core.bars.dollar_run_bars import build_dollar_run_bars
from engine.core.bars.volume_bars import build_volume_bars
from engine.core.bars.tick_bars import build_tick_bars
from engine.core.bars.tick_imbalance_bars import build_tick_imbalance_bars
from engine.core.bars.volume_imbalance_bars import build_volume_imbalance_bars
from engine.core.bars.tick_run_bars import build_tick_run_bars
from engine.core.bars.volume_run_bars import build_volume_run_bars
from utils.logger.logger import LoggerWrapper
from utils.logger.logger import log_execution


class Bars:
    def __init__(self, log_level: int):
        self.logger = LoggerWrapper(name="Bars creation module", level=log_level)
        pass

    @log_execution
    def get_tick_bars(self, bar_size: int = 100, trades_data: pl.DataFrame = None):
        return build_tick_bars(trades_data, bar_size=bar_size)

    @log_execution
    def get_volume_bars(self, bar_size: float = 1, trades_data: pl.DataFrame = None):
        return build_volume_bars(trades_data, bar_size=bar_size)

    @log_execution
    def get_dollar_bars(
        self, bar_size: float = 100000, trades_data: pl.DataFrame = None
    ):
        return build_dollar_bars(trades_data, bar_size=bar_size)

    # TODO INVESTIGATE
    # def get_time_bars(self):
    #     klines_data = self._get_klines_data()
    #     return build_time_bars(klines_data)

    @log_execution
    def get_tick_imbalance_bars(self, trades_data: pl.DataFrame = None):
        return build_tick_imbalance_bars(trades_data)

    @log_execution
    def get_volume_imbalance_bars(self, trades_data: pl.DataFrame = None):
        return build_volume_imbalance_bars(trades_data)

    @log_execution
    def get_dollar_imbalance_bars(self, trades_data: pl.DataFrame = None):
        return build_dollar_imbalance_bars(trades_data)

    @log_execution
    def get_tick_run_bars(self, trades_data: pl.DataFrame = None):
        return build_tick_run_bars(trades_data)

    @log_execution
    def get_volume_run_bars(self, trades_data: pl.DataFrame = None):
        return build_volume_run_bars(trades_data)

    @log_execution
    def get_dollar_run_bars(self, trades_data: pl.DataFrame = None):
        return build_dollar_run_bars(trades_data)
