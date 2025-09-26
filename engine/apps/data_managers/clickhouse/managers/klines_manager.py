from clickhouse_driver import Client
from utils.logger.logger import LoggerWrapper, log_execution
import polars as pl
from utils.global_variables.GLOBAL_VARIABLES import SYMBOL, TIMEFRAME


class ClickHouseKlinesManager:
    def __init__(self, client: Client, log_level: int = 10):
        self.client = client
        self.logger = LoggerWrapper(
            name="Click House Klines Manager Module", level=log_level
        )

    @log_execution
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

    def insert_klines(
        self, df: pl.DataFrame, symbol: str = SYMBOL, timeframe: str = TIMEFRAME
    ):
        table_name = f"klines_{symbol}_{timeframe}"
        rows = [tuple(row) for row in df.to_numpy()]
        self.client.execute(f"INSERT INTO {table_name} VALUES", rows)

    @log_execution
    def get_klines(
        self,
        symbol: str = SYMBOL,
        timeframe: str = TIMEFRAME,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        columns: str | None = None,
    ) -> pl.DataFrame:
        table_name = f"klines_{symbol}_{timeframe}"

        select_cols = ", ".join(columns) if columns else "*"

        conditions = []
        if start_date:
            conditions.append(f"open_time >= '{start_date}'")
        if end_date:
            conditions.append(f"open_time <= '{end_date}'")

        where_clause = ""
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)

        query = f"SELECT {select_cols} FROM {table_name}{where_clause}"

        data = self.client.execute(query)

        return data
