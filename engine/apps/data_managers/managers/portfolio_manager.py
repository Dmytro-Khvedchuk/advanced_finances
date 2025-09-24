from clickhouse_driver import Client
from engine.apps.data_managers.clickhouse.managers.portfolio_manager import ClickHousePortfolioManager


class PortfolioDataManager:
    def __init__(self, client: Client, log_level : int = 10):
        self.clickhouse_portfolio_manager = ClickHousePortfolioManager(client=client, log_level=log_level)
        self.clickhouse_portfolio_manager.create_tables()

    def insert_trade(self):
        pass

    def insert_position(self):
        pass

    def insert_order(self):
        pass

    def get_trades(self):
        pass

    def get_positions(self):
        pass

    def get_orders(self):
        pass



    