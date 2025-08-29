from engine.core.bars.dollar_bars import build_dollar_bars
from engine.core.bars.dollar_imbalance_bars import build_dollar_imbalance_bars
from engine.core.bars.dollar_run_bars import build_dollar_run_bars
from engine.core.bars.tick_bars import build_tick_bars
from engine.core.bars.tick_imbalance_bars import build_tick_imbalance_bars
from engine.core.bars.tick_run_bars import build_tick_run_bars
from engine.core.bars.volume_bars import build_volume_bars
from engine.core.bars.volume_imbalance_bars import build_volume_imbalance_bars
from engine.core.bars.volume_run_bars import build_volume_run_bars
from polars import DataFrame
from utils.logger.logger import log_execution
from utils.logger.logger import LoggerWrapper


class Bars:
    # Note! All bars here are represented with details in Lopez De Prado book Advances in Financial Machine Learning
    def __init__(self, log_level: int):
        self.logger = LoggerWrapper(name="Bars Creation Module", level=log_level)
        pass

    @log_execution
    def get_tick_bars(self, bar_size: int = 100, trades_data: DataFrame = None):
        """
        Wrapper function. Returns tick bars

        :param bar_size: Amount of ticks that represent 1 bar
        :type bar_size: int
        :param trades_data: Trades data for bars creation
        :type trades_data: pl.DataFrame | None
        :returns: pl.DataFrame with bars OHLC data
        """
        return build_tick_bars(trades_data, bar_size=bar_size)

    @log_execution
    def get_volume_bars(self, bar_size: float = 1, trades_data: DataFrame = None):
        """
        Wrapper function. Returns volume bars

        :param bar_size: Amount of volume that represent 1 bar
        :type bar_size: float
        :param trades_data: Trades data for bars creation
        :type trades_data: pl.DataFrame | None
        :returns: pl.DataFrame with bars OHLC data
        """
        return build_volume_bars(trades_data, bar_size=bar_size)

    @log_execution
    def get_dollar_bars(self, bar_size: float = 100000, trades_data: DataFrame = None):
        """
        Wrapper function. Returns dollar bars

        :param bar_size: Amount of dollars that represent 1 bar
        :type bar_size: int
        :param trades_data: Trades data for bars creation
        :type trades_data: pl.DataFrame | None
        :returns: pl.DataFrame with bars OHLC data
        """
        return build_dollar_bars(trades_data, bar_size=bar_size)

    @log_execution
    def get_tick_imbalance_bars(self, trades_data: DataFrame = None):
        """
        Wrapper function. Returns tick imbalance bars

        :param trades_data: Trades data for bars creation
        :type trades_data: pl.DataFrame | None
        :returns: pl.DataFrame with bars OHLC data
        """
        return build_tick_imbalance_bars(trades_data)

    @log_execution
    def get_volume_imbalance_bars(self, trades_data: DataFrame = None):
        """
        Wrapper function. Returns volume imbalance bars

        :param trades_data: Trades data for bars creation
        :type trades_data: pl.DataFrame | None
        :returns: pl.DataFrame with bars OHLC data
        """
        return build_volume_imbalance_bars(trades_data)

    @log_execution
    def get_dollar_imbalance_bars(self, trades_data: DataFrame = None):
        """
        Wrapper function. Returns dollar imbalance bars

        :param trades_data: Trades data for bars creation
        :type trades_data: pl.DataFrame | None
        :returns: pl.DataFrame with bars OHLC data
        """
        return build_dollar_imbalance_bars(trades_data)

    @log_execution
    def get_tick_run_bars(self, trades_data: DataFrame = None):
        """
        Wrapper function. Returns tick run bars

        :param trades_data: Trades data for bars creation
        :type trades_data: pl.DataFrame | None
        :returns: pl.DataFrame with bars OHLC data
        """
        return build_tick_run_bars(trades_data)

    @log_execution
    def get_volume_run_bars(self, trades_data: DataFrame = None):
        """
        Wrapper function. Returns volume run bars

        :param trades_data: Trades data for bars creation
        :type trades_data: pl.DataFrame | None
        :returns: pl.DataFrame with bars OHLC data
        """
        return build_volume_run_bars(trades_data)

    @log_execution
    def get_dollar_run_bars(self, trades_data: DataFrame = None):
        """
        Wrapper function. Returns dollar run bars

        :param trades_data: Trades data for bars creation
        :type trades_data: pl.DataFrame | None
        :returns: pl.DataFrame with bars OHLC data
        """
        return build_dollar_run_bars(trades_data)
