import polars as pl
import os


class ParquetManager:
    def __init__(self, file_path):
        self.file_path = file_path

    def read(self) -> pl.DataFrame:
        if os.path.exists(self.file_path):
            return pl.read_parquet(self.file_path)
        return pl.DataFrame([])

    def append(self, df: pl.DataFrame):
        existing = self.read()
        combined = pl.concat([existing, df])
        combined = combined.unique(subset=["id"])
        combined = combined.sort("id")
        combined.write_parquet(self.file_path)
        return combined
