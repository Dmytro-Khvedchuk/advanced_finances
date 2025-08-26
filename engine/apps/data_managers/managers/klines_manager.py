from utils.global_variables.GLOBAL_VARIABLES import (
    SYMBOL,
    BINANCE_TRADES_LIMIT,
    TIMEFRAME,
    TIMEFRAME_MAP,
    BINANCE_EARLIEST_DATE,
    BINANCE_LATEST_DATE,
)
from utils.global_variables.SCHEMAS import KLINES_SCHEMA
from dateutil.parser import parse
import polars as pl
from utils.logger.logger import LoggerWrapper
from utils.logger.logger import log_execution
import numpy as np
from tqdm import tqdm
from clickhouse_driver import Client as DBClient
from engine.apps.data_managers.clickhouse.data_manager import ClickHouseDataManager
from binance.client import Client as BinanceClient
from API.data_fetcher import FetchData


class KlineDataManager:
    def __init__(
        self,
        database_client: DBClient,
        binance_client: BinanceClient,
        symbol: str = SYMBOL,
        log_level: int = 10,
    ):
        self.logger = LoggerWrapper(name="Kline Data Manager Module", level=log_level)
        self.symbol = symbol
        self.data_fetcher = FetchData(
            client=binance_client, symbol=symbol, log_level=log_level
        )
        self.click_house_data_manager = ClickHouseDataManager(
            client=database_client, log_level=log_level
        )

    # TODO: refactor
    @log_execution
    def get_klines(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        timeframe: str = TIMEFRAME,
    ):
        self.click_house_data_manager.klines.create_klines_table(
            symbol=self.symbol, timeframe=timeframe
        )

        if start_date:
            start_date = self._parse_date_for_klines(start_date)
        if end_date:
            end_date = self._parse_date_for_klines(end_date)

        present_time_in_db = pl.DataFrame(
            self.click_house_data_manager.klines.get_klines(
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
            self.click_house_data_manager.klines.get_klines(
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

                self.click_house_data_manager.klines.insert_klines(
                    df=data, symbol=self.symbol
                )

                start = int(data["open_time"].max().timestamp() * 1000) + interval_ms

    # ---=== HELPER METHODS ===---
    def _parse_date_for_klines(self, date: str = "22 Oct 2024"):
        try:
            parsed_date = parse(date)
            timestamp_ms = int(parsed_date.timestamp() * 1000)
            return timestamp_ms
        except Exception as fallback_error:
            self.logger.error(f"Failed to parse date '{date}': {fallback_error}. ")
            raise ValueError(f"Failed to parse date '{date}': {fallback_error}. ")

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
