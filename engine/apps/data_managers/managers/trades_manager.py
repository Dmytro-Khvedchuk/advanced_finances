from API.data_fetcher import FetchData
from binance.client import Client as BinanceClient
from clickhouse_driver import Client as DBClient
from engine.apps.data_managers.clickhouse.data_manager import ClickHouseDataManager
from numpy import arange, append, diff, insert, int64, ndarray, setxor1d, sort, where
from polars import DataFrame
from tqdm import tqdm
from typing import Any
from utils.global_variables.GLOBAL_VARIABLES import (
    BINANCE_EARLIEST_ID,
    BINANCE_TRADES_LIMIT,
    SYMBOL,
)
from utils.global_variables.SCHEMAS import TRADES_SCHEMA
from utils.logger.logger import LoggerWrapper
from utils.logger.logger import log_execution


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
        self.data_fetcher = FetchData(
            client=binance_client, symbol=symbol, log_level=log_level
        )
        self.click_house_data_manager = ClickHouseDataManager(
            client=database_client, log_level=log_level
        )

    @log_execution
    def get_trades(
        self, *, start_id: int | None = None, end_id: int | None = None
    ) -> DataFrame:
        self.click_house_data_manager.trades.create_trades_table(symbol=self.symbol)

        present_ids_in_db = DataFrame(
            self.click_house_data_manager.trades.get_trades(
                symbol=self.symbol, start_id=start_id, end_id=end_id, columns=["id"]
            ),
            schema=["id"],
            orient="row",
        )["id"].to_numpy()

        expected_ids, start_id, end_id = self._generate_expected_ids(
            start_id=start_id, end_id=end_id
        )

        ids_to_fetch = setxor1d(present_ids_in_db, expected_ids)
        ids_to_fetch = sort(ids_to_fetch)

        if ids_to_fetch.size > 0:
            self.logger.info("Missing data in the dataframe. Fetching...")
            fetch_ids_dictionary = self._get_consecutive_trades(arr=ids_to_fetch)
            self._fetch_and_write_trades(fetch_ids_dictionary=fetch_ids_dictionary)

        data = DataFrame(
            self.click_house_data_manager.trades.get_trades(
                symbol=self.symbol,
                start_id=start_id,
                end_id=end_id,
            ),
            schema=TRADES_SCHEMA,
            orient="row",
        ).sort(by="id")

        return data

    def _generate_expected_ids(
        self, *, start_id: int | None = None, end_id: int | None = None
    ):
        if start_id is None:
            start_id = BINANCE_EARLIEST_ID
        if end_id is None:
            end_id = self.data_fetcher.fetch_recent_trades(limit=1)[0]["id"]

        return arange(start_id, end_id + 1, dtype=int64), start_id, end_id

    # ---=== HELPER METHODS ===---
    def _fetch_and_write_trades(self, fetch_ids_dictionary: dict):
        """Fetch trades from API and append them to parquet storage with retry logic."""
        for from_id, amount in fetch_ids_dictionary.items():
            fetch_points, limits = self._calculate_fetch_points(amount, from_id)
            desc = f"Fetching trades from {from_id} (total {amount})"
            for range_id, limit in tqdm(
                zip(fetch_points, limits), total=len(fetch_points), desc=desc
            ):
                data = DataFrame(
                    self.data_fetcher.fetch_historical_trades(
                        from_id=int(range_id), limit=int(limit)
                    ),
                    orient="row",
                    schema=TRADES_SCHEMA,
                ).rename(
                    {
                        "quoteQty": "quote_qty",
                        "isBuyerMaker": "is_buyer_maker",
                        "isBestMatch": "is_best_match",
                    }
                )

                if len(data) == 0:
                    break

                self.click_house_data_manager.trades.insert_trades(
                    df=data, symbol=self.symbol
                )

    # ---=== STATIC METHODS ===---
    @staticmethod
    def _calculate_fetch_points(amount: int, from_id: int):
        """Calculate fetch start points and limits based on Binance API constraints."""
        if amount > BINANCE_TRADES_LIMIT:
            fetch_points = arange(from_id, from_id + amount, BINANCE_TRADES_LIMIT)
            limits = [BINANCE_TRADES_LIMIT] * len(fetch_points)
        else:
            fetch_points = [from_id]
            limits = [amount]

        return fetch_points, limits

    @staticmethod
    def _get_consecutive_trades(arr: ndarray) -> dict[Any, Any]:
        breaks = where(diff(arr) != 1)[0]

        starts_idx = insert(breaks + 1, 0, 0)
        ends_idx = append(breaks, len(arr) - 1)

        lengths = ends_idx - starts_idx + 1
        starts = arr[starts_idx]

        result = dict(zip(starts, lengths))
        return result
