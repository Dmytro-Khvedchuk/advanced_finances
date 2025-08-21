from clickhouse_driver import Client
from utils.global_variables.GLOBAL_VARIABLES import TIMEFRAME, SYMBOL
import polars as pl


class ClickHouseDataManager:
    def __init__(self, client: Client):
        self.client = client

    def create_klines_table(self, symbol: str = SYMBOL, timeframe: str = TIMEFRAME):
        table_name = f"klines_{symbol}_{timeframe}"

        self.client.execute(
            f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            open_time Int64,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume Float64,
            close_time Int64,
            quote_asset_volume Float64,
            num_trades Int64,
            taker_buy_base_asset_volume Float64,
            taker_buy_quote_asset_volume Float64,
            ignore String
        )
        ENGINE = MergeTree()
        ORDER BY open_time
        PRIMARY KEY open_time
        """
        )

        return table_name

    def insert_klines(
        self, df: pl.DataFrame, symbol: str = SYMBOL, timeframe: str = TIMEFRAME
    ):
        table_name = f"klines_{symbol}_{timeframe}"
        rows = [tuple(row) for row in df.to_numpy()]
        self.client.execute(f"INSERT INTO {table_name} VALUES", rows)

    def get_klines(
        self,
        symbol: str = SYMBOL,
        timeframe: str = TIMEFRAME,
        *,
        start_date: str = None,
        end_date: str = None,
    ) -> pl.DataFrame:
        table_name = f"klines_{symbol}_{timeframe}"

        data = self.client.execute(f"SELECT * FROM {table_name}")
        return data
