# TODO: this is the main backtest class that will do an ETL process and generate reports.

class BackTest:
    def __init__(self, client, symbol):
        self.client = client
        self.symbol = symbol