from API.data_fetcher import FetchData
from binance.client import Client as BinanceClient
from clickhouse_driver import Client as DBClient
from dateutil.parser import parse
from datetime import timedelta
from datetime import timezone
from engine.apps.data_managers.clickhouse.data_manager import ClickHouseDataManager
from numpy import arange, int64, ndarray, setxor1d, sort
from polars import DataFrame
from tqdm import tqdm
from utils.global_variables.GLOBAL_VARIABLES import (
    BINANCE_EARLIEST_DATE,
    BINANCE_LATEST_DATE,
    BINANCE_TRADES_LIMIT,
    SYMBOL,
    TIMEFRAME,
    TIMEFRAME_MAP,
)
from utils.global_variables.SCHEMAS import KLINES_SCHEMA
from utils.logger.logger import LoggerWrapper
from utils.logger.logger import log_execution


class KlineDataManager:
    def __init__(
        self,
        database_client: DBClient,
        data_fetcher: FetchData,
        symbol: str = SYMBOL,
        log_level: int = 10,
    ):
        self.logger = LoggerWrapper(name="Kline Data Manager Module", level=log_level)
        self.symbol = symbol
        self.data_fetcher = data_fetcher
        self.click_house_data_manager = ClickHouseDataManager(
            client=database_client, log_level=log_level
        )

    @log_execution
    def get_klines(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        timeframe: str = TIMEFRAME,
    ):
        """
        Function that returns polars dataframe of klines.
        If data is not in database, it fetches necessary data from API

        :param start_date: Starting date of data
        :type start_date: str | None
        :param end_date: Ending date of data
        :type end_date: str | None
        :param timeframe: timeframe of data
        :type timeframe: str
        :returns: pl.DataFrame with all requested data
        """
        self.click_house_data_manager.klines.create_klines_table(
            symbol=self.symbol, timeframe=timeframe
        )

        if start_date:
            start_date = self._parse_date_for_klines(start_date)
        if end_date:
            end_date = self._parse_date_for_klines(end_date)

        present_time_in_db = DataFrame(
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

        timestamps_to_fetch = setxor1d(present_time_in_db, expected_time_in_db)
        timestamps_to_fetch = sort(timestamps_to_fetch)

        if timestamps_to_fetch.size > 0:
            self.logger.info("Missing data in the dataframe. Fetching...")
            elements = {}
            N = BINANCE_TRADES_LIMIT
            for i in range(0, len(timestamps_to_fetch), N):
                from_ts = timestamps_to_fetch[i]
                to_ts = timestamps_to_fetch[min(i + N, len(timestamps_to_fetch)) - 1]
                elements[from_ts] = to_ts

            self._fetch_and_write_klines(fetch_dictionary=elements, timeframe=timeframe)

        data = DataFrame(
            self.click_house_data_manager.klines.get_klines(
                symbol=self.symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
            ),
            schema=KLINES_SCHEMA,
            orient="row",
        ).sort(by="open_time")

        return data

    # ---=== HELPER METHODS ===---
    @log_execution
    def _fetch_and_write_klines(
        self, fetch_dictionary: dict, timeframe: str = TIMEFRAME
    ):
        """
        Helper function. Fetches data with Data Fetcher Module that connects to Binance API

        :param fetch_dictionary: dictionary that contains ranges in ms {start_time_ms: end_time_ms}
        :type fetch_dictionary: dict
        :param timeframe: timeframe of the klines
        :type timeframe: str
        """
        interval_ms = TIMEFRAME_MAP[timeframe]
        for from_ts, to_ts in tqdm(fetch_dictionary.items(), desc="Fetching klines"):
            start = int(from_ts)
            to_ts = int(to_ts)
            while start <= to_ts:
                data = DataFrame(
                    self.data_fetcher.fetch_historical_klines(
                        timeframe=timeframe, start_str=start, end_str=to_ts
                    ),
                    orient="row",
                    schema=KLINES_SCHEMA,
                )
                if len(data) == 0:
                    break

                self.click_house_data_manager.klines.insert_klines(
                    df=data, symbol=self.symbol, timeframe=timeframe
                )

                if isinstance(interval_ms, timedelta):
                    interval_ms = int(interval_ms.total_seconds() * 1000)

                start = int(data["open_time"].max()) + interval_ms

    def _parse_date_for_klines(self, date: str = "22 Oct 2024"):
        """
        Helper function. Parses string date format into UNIX ms

        :param date: date in string format like 22 Oct 2024
        :type date: str
        :returns: timestamp in UNIX ms format
        """
        try:
            parsed_date = parse(date)

            if parsed_date.tzinfo is None:
                parsed_date = parsed_date.replace(tzinfo=timezone.utc)
            else:
                parsed_date = parsed_date.astimezone(timezone.utc)

            timestamp_ms = int(parsed_date.timestamp() * 1000)
            return timestamp_ms
        except Exception as fallback_error:
            self.logger.error(f"Failed to parse date '{date}': {fallback_error}. ")
            raise ValueError(f"Failed to parse date '{date}': {fallback_error}. ")

    def _generate_expected_timestamps(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        timeframe: str = TIMEFRAME,
    ) -> ndarray:
        """
        Generate expected timestamps (in ms) between "start_date" and "end_date"
        based on "timeframe" (1m, 1h, 1D, ...).

        :param start_date: Starting date of data
        :type start_date: str | None
        :param end_date: Ending date of data
        :type end_date: str | None
        :param timframe: Binance timeframe of klines
        :type timeframe: str
        :returns: np.ndarray with timestamps in UNIX ms
        """

        if start_date is None:
            start_date = self._parse_date_for_klines(BINANCE_EARLIEST_DATE)

        if end_date is None:
            end_date = self._parse_date_for_klines(BINANCE_LATEST_DATE)

        if timeframe not in TIMEFRAME_MAP:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        step_ms = int(TIMEFRAME_MAP[timeframe].total_seconds() * 1000)

        return arange(start_date, end_date + 1, step_ms, dtype=int64)
