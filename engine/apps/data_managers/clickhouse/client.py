from clickhouse_driver import Client
from os import getenv


def get_clickhouse_client() -> Client:
    """
    With .env file receiving client of clickhouse of docker

    :returns: Client
    """
    password = getenv("CLICKHOUSE_PASSWORD") or None

    return Client(
        host=getenv("CLICKHOUSE_HOST"),
        port=int(getenv("CLICKHOUSE_PORT")),
        user=getenv("CLICKHOUSE_USER"),
        password=password,
        database=getenv("CLICKHOUSE_DB"),
    )
