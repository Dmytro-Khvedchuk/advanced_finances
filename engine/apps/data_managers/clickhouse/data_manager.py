from clickhouse_driver import Client
from engine.apps.data_managers.clickhouse.managers.klines_manager import (
    ClickHouseKlinesManager,
)
from utils.global_variables.GLOBAL_VARIABLES import TIMEFRAME, SYMBOL
import polars as pl

from utils.logger.logger import LoggerWrapper


class ClickHouseDataManager:
    def __init__(self, client: Client, log_level: int = 10):
        self.client = client
        self.logger = LoggerWrapper(
            name="Click House Data Manager Module", level=log_level
        )
        self.klines = ClickHouseKlinesManager(client=client, log_level=log_level)

