from API.data_fetcher import FetchData
from binance.client import Client as BinanceClient
from clickhouse_driver import Client as DBClient
from engine.apps.data_managers.clickhouse.data_manager import ClickHouseDataManager
from engine.apps.data_managers.managers.klines_manager import KlineDataManager
from engine.apps.data_managers.managers.trades_manager import TradeDataManager
from engine.apps.data_managers.managers.portfolio_manager import PortfolioDataManager
from utils.global_variables.GLOBAL_VARIABLES import (
    SYMBOL,
)
from utils.logger.logger import LoggerWrapper, log_execution


class MarketDataManager:
    def __init__(
        self,
        binance_client: BinanceClient,
        database_client: DBClient,
        symbol: str = SYMBOL,
        log_level: int = 10,
    ):
        self.logger = LoggerWrapper(name="Market Data Manager Module", level=log_level)
        self.click_house_data_manager = ClickHouseDataManager(
            client=database_client, log_level=log_level
        )
        self.data_fetcher = FetchData(
            client=binance_client, symbol=symbol, log_level=log_level
        )
        self.kline_manager = KlineDataManager(
            database_client=database_client,
            data_fetcher=self.data_fetcher,
            symbol=symbol,
            log_level=log_level,
        )
        self.trade_manager = TradeDataManager(
            database_client=database_client,
            data_fetcher=self.data_fetcher,
            symbol=symbol,
            log_level=log_level,
        )
        self.portfolio_manager = PortfolioDataManager(
            client=database_client, 
            log_level=log_level,
        )

    @log_execution
    def update_symbol(self, symbol: str = SYMBOL):
        self.symbol = symbol
        self.kline_manager.symbol = symbol
        self.trade_manager.symbol = symbol
        self.data_fetcher.symbol = symbol
