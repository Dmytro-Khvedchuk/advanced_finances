import polars as pl
import os
from utils.logger.logger import LoggerWrapper
from utils.logger.logger import log_execution


class ParquetManager:
    def __init__(self, file_path, log_level: int = 10):
        self.file_path = file_path
        self.logger = LoggerWrapper(name="Parquet Manager Module", level=log_level)

    @log_execution
    def read_trades(self) -> pl.DataFrame:
        if os.path.exists(self.file_path):
            return pl.read_parquet(self.file_path)
        self.logger.warning(f"File {self.file_path} does not exist. Returning empty DataFrame.")
        return pl.DataFrame([])

    @log_execution
    def append_trades(self, df: pl.DataFrame):
        existing = self.read_trades()
        combined = pl.concat([existing, df])
        combined = combined.unique(subset=["id"])
        combined = combined.sort("id")
        combined.write_parquet(self.file_path)
        return combined

    @log_execution
    def read_klines(self) -> pl.DataFrame:
        if os.path.exists(self.file_path):
            return pl.read_parquet(self.file_path)
        self.logger.warning(f"File {self.file_path} does not exist. Returning empty DataFrame.")
        return pl.DataFrame([])

