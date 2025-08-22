import polars as pl
import numpy as np
from binance.client import Client as BinanceClient
from clickhouse_driver import Client as DBClient
from API.data_fetcher import FetchData
from typing import Any
from engine.apps.data_managers.clickhouse.data_manager import ClickHouseDataManager
from utils.global_variables.GLOBAL_VARIABLES import (
    BINANCE_TRADES_LIMIT,
    SYMBOL,
    TIMEFRAME,
    TIMEFRAME_MAP,
    BINANCE_EARLIEST_DATE,
    BINANCE_LATEST_DATE,
)
from utils.global_variables.SCHEMAS import KLINES_SCHEMA
from tqdm import tqdm
from utils.logger.logger import LoggerWrapper
from utils.logger.logger import log_execution
from dateutil.parser import parse
from datetime import datetime


class MarketDataManager:
    def __init__(
        self,
        binance_client: BinanceClient,
        database_client: DBClient,
        symbol: str = SYMBOL,
        log_level: int = 10,
    ):
        self.logger = LoggerWrapper(name="Market Data Manager Module", level=log_level)
        self.symbol = symbol
        self.click_house_data_manager = ClickHouseDataManager(
            client=database_client, log_level=log_level
        )
        self.data_fetcher = FetchData(
            client=binance_client, symbol=symbol, log_level=log_level
        )

    # TODO: rewrite
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

    @log_execution
    def get_klines(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        timeframe: str = TIMEFRAME,
    ):
        if start_date:
            start_date = self._parse_date_for_klines(start_date)
        if end_date:
            end_date = self._parse_date_for_klines(end_date)

        present_time_in_db = pl.DataFrame(
            self.click_house_data_manager.get_klines(
                symbol=self.symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
                columns=["open_time"],
            ),
            schema=["open_time"],
            orient="row",
        )["open_time"].to_numpy()

        expected_time_in_db = self._generate_expected_timestamps(
            start_date=start_date, end_date=end_date, timeframe=timeframe
        )

        timestamps_to_fetch = np.setxor1d(present_time_in_db, expected_time_in_db)
        timestamps_to_fetch = np.sort(timestamps_to_fetch)

        if timestamps_to_fetch.size > 0:
            self.logger.info("Missing data in the dataframe. Fetching...")
            elements = {}
            N = BINANCE_TRADES_LIMIT
            for i in range(0, len(timestamps_to_fetch), N):
                from_ts = timestamps_to_fetch[i]
                to_ts = timestamps_to_fetch[min(i + N, len(timestamps_to_fetch)) - 1]
                elements[from_ts] = to_ts

            self._fetch_and_write_klines(fetch_dictionary=elements, timeframe=timeframe)

        data = pl.DataFrame(
            self.click_house_data_manager.get_klines(
                symbol=self.symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
            ),
            schema=KLINES_SCHEMA,
            orient="row",
        )

        return data

    @log_execution
    def get_order_book(self):
        pass

    # ---=== HELPER METHODS ===---
    def _parse_date_for_klines(self, date: str = "22 Oct 2024"):
        try:
            parsed_date = parse(date)
            timestamp_ms = int(parsed_date.timestamp() * 1000)
            return timestamp_ms
        except Exception as fallback_error:
            self.logger.error(f"Failed to parse date '{date}': {fallback_error}. ")
            raise ValueError(f"Failed to parse date '{date}': {fallback_error}. ")

    @log_execution
    def _fetch_and_write_klines(
        self, fetch_dictionary: dict, timeframe: str = TIMEFRAME
    ):
        interval_ms = TIMEFRAME_MAP[timeframe]  # перетворення 1h → 3600000 ms
        for from_ts, to_ts in tqdm(fetch_dictionary.items(), desc="Fetching klines"):
            start = from_ts
            while start <= to_ts:
                data = pl.DataFrame(
                    self.data_fetcher.fetch_historical_klines(
                        timeframe=timeframe, start_str=start, end_str=to_ts
                    ),
                    orient="row",
                    schema=KLINES_SCHEMA,
                )
                if len(data) == 0:
                    break

                self.click_house_data_manager.insert_klines(df=data, symbol=self.symbol)

                # Переходимо до наступного batch
                start = int(data["open_time"].max()) + interval_ms

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
    def _generate_expected_timestamps(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        timeframe: str = TIMEFRAME,
    ) -> np.ndarray:
        """
        Generate expected timestamps (in ms) between start_date and end_date
        based on timeframe (1m, 1h, 1D, ...), fast with NumPy.
        """

        if start_date is None:
            start_date = self._parse_date_for_klines(BINANCE_EARLIEST_DATE)

        if end_date is None:
            end_date = self._parse_date_for_klines(BINANCE_LATEST_DATE)

        if timeframe not in TIMEFRAME_MAP:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        step_ms = int(TIMEFRAME_MAP[timeframe].total_seconds() * 1000)

        return np.arange(start_date, end_date + 1, step_ms, dtype=np.int64)

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
