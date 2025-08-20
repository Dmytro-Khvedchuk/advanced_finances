import polars as pl
import os
from utils.logger.logger import LoggerWrapper
from utils.logger.logger import log_execution
from utils.global_variables.SCHEMAS import KLINES_SCHEMA, TRADES_SCHEMA


class ParquetManager:
    def __init__(self, file_path, interval : str = "1h", log_level: int = 10):
        self.file_path = file_path
        self.logger = LoggerWrapper(name="Parquet Manager Module", level=log_level)
        self.trades_path = f"{file_path}_trades.parquet"
        self.klines_path = f"{file_path}_klines_{interval}.parquet"

    @log_execution
    def read_trades(self) -> pl.DataFrame:
        if os.path.exists(self.trades_path):
            return pl.read_parquet(self.trades_path)
        df = pl.DataFrame([], schema=TRADES_SCHEMA)
        df.write_parquet(self.trades_path)
        self.logger.warning(
            f"File {self.trades_path} does not exist. Returning empty DataFrame."
        )
        return df

    @log_execution
    def append_trades(self, df: pl.DataFrame):
        existing = self.read_trades()
        combined = pl.concat([existing, df])
        combined = combined.unique(subset=["id"])
        combined = combined.sort("id")
        combined.write_parquet(self.trades_path)
        return combined

    @log_execution
    def read_klines(self) -> pl.DataFrame:
        if os.path.exists(self.klines_path):
            return pl.read_parquet(self.klines_path)
        df = pl.DataFrame([], schema=KLINES_SCHEMA)
        df.write_parquet(self.klines_path)
        self.logger.warning(
            f"File {self.klines_path} does not exist. Returning empty DataFrame."
        )
        return df

    @log_execution
    def append_klines(self, df: pl.DataFrame):
        existing = self.read_klines()
        combined = pl.concat([existing, df])
        combined = combined.unique(subset=["open_time"])
        combined = combined.sort("open_time")
        combined.write_parquet(self.klines_path)
        return combined
