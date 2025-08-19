import polars as pl
from binance.client import Client
from API.data_fetcher import FetchData
from engine.apps.data_managers.parquet_manager import ParquetManager
from utils.global_variables.GLOBAL_VARIABLES import DATA_PATH
import numpy as np
from typing import Any
from utils.global_variables.GLOBAL_VARIABLES import (
    BINANCE_TRADES_LIMIT,
    MAX_RETRIES,
    RETRY_DELAY,
)
from tqdm import tqdm
from requests.exceptions import ReadTimeout
from time import sleep


class MarketDataManager:
    def __init__(self, client: Client, symbol: str = "BTCUSDT"):
        self.parquet_storage = ParquetManager(DATA_PATH / f"{symbol}.parquet")
        self.data_fetcher = FetchData(client=client, symbol=symbol)

    def get_trades(self, *, start_id: int = None, end_id: int = None) -> pl.DataFrame:
        df = self.parquet_storage.read()

        if df.is_empty():
            return df

        first_trade_id = df["id"].min()
        last_binance_trade_id = self.data_fetcher.fetch_recent_trades(limit=1)[0]["id"]

        self._validate_range(start_id, end_id, first_trade_id, last_binance_trade_id)

        if start_id is not None and end_id is None:
            lacking_ids = self._find_lacking_ids(df, start_id, last_binance_trade_id)
            fetch_ids_dictionary = self._get_consecutive_trades(lacking_ids)
            self._fetch_and_write(fetch_ids_dictionary)
            final_df = self.parquet_storage.read()
            return final_df.filter(pl.col("id") >= start_id)

        elif start_id is None and end_id is not None:
            lacking_ids = self._find_lacking_ids(df, first_trade_id, end_id)
            fetch_ids_dictionary = self._get_consecutive_trades(lacking_ids)
            self._fetch_and_write(fetch_ids_dictionary)
            final_df = self.parquet_storage.read()
            return final_df.filter(pl.col("id") <= end_id)

        elif start_id is not None and end_id is not None:
            lacking_ids = self._find_lacking_ids(df, start_id, end_id)
            fetch_ids_dictionary = self._get_consecutive_trades(lacking_ids)
            self._fetch_and_write(fetch_ids_dictionary)
            final_df = self.parquet_storage.read()
            return final_df.filter(
                (pl.col("id") >= start_id) & (pl.col("id") <= end_id)
            )

        else:
            lacking_ids = self._find_lacking_ids(
                df, first_trade_id, last_binance_trade_id
            )
            fetch_ids_dictionary = self._get_consecutive_trades(lacking_ids)
            self._fetch_and_write(fetch_ids_dictionary)
            return self.parquet_storage.read()

    def _fetch_and_write(self, fetch_ids_dictionary: dict):
        """Fetch trades from API and append them to parquet storage with retry logic."""
        for from_id, amount in fetch_ids_dictionary.items():
            fetch_points, limits = self._calculate_fetch_points(amount, from_id)

            desc = f"Fetching trades from {from_id} (total {amount})"
            for range_id, limit in tqdm(
                zip(fetch_points, limits), total=len(fetch_points), desc=desc
            ):
                self._fetch_with_retry(range_id, limit)

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

    def _fetch_with_retry(self, from_id: int, limit: int):
        """Fetch a batch of trades with retry on ReadTimeout."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                data = pl.DataFrame(
                    self.data_fetcher.fetch_historical_trades(
                        from_id=from_id, limit=limit
                    )
                )
                self.parquet_storage.append(data)
                break
            except ReadTimeout:
                if attempt < MAX_RETRIES:
                    # should be logged
                    print(
                        f"ReadTimeout, retrying {attempt}/{MAX_RETRIES} after {RETRY_DELAY}s..."
                    )
                    sleep(RETRY_DELAY)
                else:
                    print(
                        f"Failed to fetch trades from {from_id} after {MAX_RETRIES} attempts."
                    )
                    raise

    def get_klines(self):
        pass

    def get_order_book(self):
        pass

    # ---=== HELPER METHODS ===---

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

        # Indices of run starts
        starts_idx = np.insert(breaks + 1, 0, 0)  # add 0 for the first element
        # Indices of run ends
        ends_idx = np.append(breaks, len(arr) - 1)

        # Run lengths
        lengths = ends_idx - starts_idx + 1

        # Starting values of each run
        starts = arr[starts_idx]

        # Create dictionary
        result = dict(zip(starts, lengths))
        return result
