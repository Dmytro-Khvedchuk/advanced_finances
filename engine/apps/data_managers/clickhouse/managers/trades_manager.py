from clickhouse_driver import Client
from utils.logger.logger import LoggerWrapper, log_execution
import polars as pl
from utils.global_variables.GLOBAL_VARIABLES import SYMBOL
import numpy as np


class ClickHouseTradesManager:
    def __init__(self, client: Client, log_level: int = 10):
        self.client = client
        self.logger = LoggerWrapper(
            name="Click House Trades Manager Module", level=log_level
        )

    @log_execution
    def create_trades_table(self, symbol: str = SYMBOL):
        table_name = f"trades_{symbol}"

        self.client.execute(
            f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id Int64,
            price Float64,
            qty Float64,
            quote_qty Float64,
            time Int64,
            is_buyer_maker UInt8,
            is_best_match UInt8
        )
        ENGINE = MergeTree()
        ORDER BY id
        PRIMARY KEY id
        """
        )

    @log_execution
    def insert_trades(self, df: pl.DataFrame, symbol: str = SYMBOL):
        table_name = f"trades_{symbol}"

        rows = [
            (
                int(row[0]),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                int(row[4]),
                int(row[5]),
                int(row[6]),
            )
            for row in df.to_numpy()
        ]

        self.client.execute(f"INSERT INTO {table_name} VALUES", rows)

    @log_execution
    def get_trades(
        self,
        symbol: str = SYMBOL,
        *,
        start_id: str | None = None,
        end_id: str | None = None,
        columns: str | None = None,
    ) -> pl.DataFrame:
        table_name = f"trades_{symbol}"

        select_cols = ", ".join(columns) if columns else "*"

        conditions = []
        if start_id:
            conditions.append(f"id >= '{start_id}'")
        if end_id:
            conditions.append(f"id <= '{end_id}'")

        where_clause = ""
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)

        query = f"SELECT {select_cols} FROM {table_name}{where_clause}"

        data = self.client.execute(query)

        return data
