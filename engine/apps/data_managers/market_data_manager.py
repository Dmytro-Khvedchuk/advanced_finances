import polars as pl
from binance.client import Client
from API.data_fetcher import FetchData
from engine.apps.data_managers.parquet_manager import ParquetManager
from utils.global_variables.GLOBAL_VARIABLES import DATA_PATH
import numpy as np


class MarketDataManager:
    def __init__(self, client: Client, symbol: str = "BTCUSDT"):
        self.parquet_storage = ParquetManager(DATA_PATH / f"{symbol}.parquet")
        self.data_fetcher = FetchData(client=client, symbol=symbol)

    # TODO: implement indexed search
    def get_trades(self, *, start_id: int = None, end_id: int = None) -> pl.DataFrame:
        # df fetches only for the last id, this should be optimized
        df = self.parquet_storage.read()

        if df.is_empty():
            return df

        last_trade_id = df["id"].max()

        last_binance_trade_id = self.data_fetcher.fetch_recent_trades(limit=1)[0]["id"]

        # assuming that last_binance_trade_id will be always >= last_trade_id

        # + 1 to not refetch trade that we already have
        ranges_to_fetch = np.arange(last_trade_id + 1, last_binance_trade_id, 1000)

        for range_id in ranges_to_fetch:
            data = pl.DataFrame(
                self.data_fetcher.fetch_historical_trades(from_id=range_id, limit=1000)
            )
            self.parquet_storage.append(data)

        return self.parquet_storage.read()

    def get_klines(self):
        pass

    def get_order_book(self):
        pass
