from engine.apps.data_managers.clickhouse.data_manager import ClickHouseDataManager
from utils.logger.logger import LoggerWrapper
from utils.logger.logger import log_execution
from utils.global_variables.GLOBAL_VARIABLES import SYMBOL, BINANCE_TRADES_LIMIT
import polars as pl
from binance.client import Client as BinanceClient
from clickhouse_driver import Client as DBClient
from API.data_fetcher import FetchData
import numpy as np
from tqdm import tqdm
from typing import Any


class TradeDataManager:
    def __init__(
        self,
        database_client: DBClient,
        binance_client: BinanceClient,
        symbol: str = SYMBOL,
        log_level: int = 10,
    ):
        self.logger = LoggerWrapper(name="Trade Data Manager Module", level=log_level)
        self.symbol = symbol
        self.click_house_data_manager = ClickHouseDataManager(
            client=database_client, log_level=log_level
        )
        self.data_fetcher = FetchData(
            client=binance_client, symbol=symbol, log_level=log_level
        )

    @log_execution
    def get_trades(
        self, *, start_id: int | None = None, end_id: int | None = None
    ) -> pl.DataFrame:
        df = self.parquet_storage.read_trades()

        if df.is_empty():
            return df

        first_trade_id = df["id"].min()

        last_binance_trade_id = self.data_fetcher.fetch_recent_trades(limit=1)[0]["id"]

        self._validate_range(start_id, end_id, first_trade_id, last_binance_trade_id)

        if start_id is not None and end_id is None:
            lacking_ids = self._find_lacking_ids(df, start_id, last_binance_trade_id)
            fetch_ids_dictionary = self._get_consecutive_trades(lacking_ids)
            self._fetch_and_write_trades(fetch_ids_dictionary)
            final_df = self.parquet_storage.read_trades()
            return final_df.filter(pl.col("id") >= start_id)

        elif start_id is None and end_id is not None:
            lacking_ids = self._find_lacking_ids(df, first_trade_id, end_id)
            fetch_ids_dictionary = self._get_consecutive_trades(lacking_ids)
            self._fetch_and_write_trades(fetch_ids_dictionary)
            final_df = self.parquet_storage.read_trades()
            return final_df.filter(pl.col("id") <= end_id)

        elif start_id is not None and end_id is not None:
            lacking_ids = self._find_lacking_ids(df, start_id, end_id)
            fetch_ids_dictionary = self._get_consecutive_trades(lacking_ids)
            self._fetch_and_write_trades(fetch_ids_dictionary)
            final_df = self.parquet_storage.read_trades()
            return final_df.filter(
                (pl.col("id") >= start_id) & (pl.col("id") <= end_id)
            )

        else:
            lacking_ids = self._find_lacking_ids(
                df, first_trade_id, last_binance_trade_id
            )
            fetch_ids_dictionary = self._get_consecutive_trades(lacking_ids)
            self._fetch_and_write_trades(fetch_ids_dictionary)
            return self.parquet_storage.read_trades()

    # ---=== HELPER METHODS ===---
    def _fetch_and_write_trades(self, fetch_ids_dictionary: dict):
        """Fetch trades from API and append them to parquet storage with retry logic."""
        for from_id, amount in fetch_ids_dictionary.items():
            fetch_points, limits = self._calculate_fetch_points(amount, from_id)

            desc = f"Fetching trades from {from_id} (total {amount})"
            for range_id, limit in tqdm(
                zip(fetch_points, limits), total=len(fetch_points), desc=desc
            ):
                self.data_fetcher.fetch_historical_trades(range_id, limit)

    # ---=== STATIC METHODS ===---
    @staticmethod
    def _calculate_fetch_points(amount: int, from_id: int):
        """Calculate fetch start points and limits based on Binance API constraints."""
        if amount > BINANCE_TRADES_LIMIT:
            fetch_points = np.arange(from_id, from_id + amount, BINANCE_TRADES_LIMIT)
            limits = [BINANCE_TRADES_LIMIT] * len(fetch_points)
        else:
            fetch_points = [from_id]
            limits = [amount]

        return fetch_points, limits

    @staticmethod
    def _find_lacking_ids(df, start, end):
        """Return missing trade IDs between start and end."""
        all_df_ids = df.filter((pl.col("id") >= start) & (pl.col("id") <= end))[
            "id"
        ].to_numpy()
        ids_to_have = np.arange(start, end)
        return np.setxor1d(all_df_ids, ids_to_have)

    @staticmethod
    def _validate_range(start_id, end_id, first_trade_id, last_binance_trade_id):
        if start_id is not None and start_id <= 0:
            raise IndexError("Range out of bounds")
        if end_id is not None and end_id >= last_binance_trade_id:
            raise IndexError("Range out of bounds")
        if end_id is not None and end_id < first_trade_id:
            raise IndexError(
                "Range out of bounds, end_id is less than first_trade_id in parquet file"
            )

    @staticmethod
    def _get_consecutive_trades(arr: np.ndarray) -> dict[Any, Any]:
        breaks = np.where(np.diff(arr) != 1)[0]

        starts_idx = np.insert(breaks + 1, 0, 0)
        ends_idx = np.append(breaks, len(arr) - 1)

        lengths = ends_idx - starts_idx + 1
        starts = arr[starts_idx]

        result = dict(zip(starts, lengths))
        return result
