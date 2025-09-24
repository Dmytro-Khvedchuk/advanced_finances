from clickhouse_driver import Client
from utils.logger.logger import LoggerWrapper, log_execution
from utils.global_variables.SCHEMAS import POSITIONS_SCHEMA, TRADE_HISTORY_SCHEMA, ORDER_HISTORY_SCHEMA

class ClickHousePortfolioManager:
    def __init__(self, client: Client, log_level: int = 10):
        self.client = client
        self.logger = LoggerWrapper(
            name="Click House Portfolio Manager Module", level=log_level
        )

        self.table_names_schemas = {
            "trade_history": TRADE_HISTORY_SCHEMA,
            "order_history": ORDER_HISTORY_SCHEMA,
            "current_positions": POSITIONS_SCHEMA
        }

    @log_execution
    def create_tables(self):
        for name, schema in self.table_names_schemas.items():
            columns = ",\n    ".join([f"{name} {dtype}" for name, dtype in schema.items()])

            self.client.execute(f"""
            CREATE TABLE IF NOT EXISTS {name} (
                {columns}
            )
            ENGINE = MergeTree()
            ORDER BY order_id
            PRIMARY KEY order_id
            """)