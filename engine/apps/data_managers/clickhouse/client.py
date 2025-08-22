from os import getenv
from clickhouse_driver import Client


def get_clickhouse_client() -> Client:
    password = getenv("CLICKHOUSE_PASSWORD") or None

    return Client(
        host=getenv("CLICKHOUSE_HOST"),
        port=int(getenv("CLICKHOUSE_PORT")),
        user=getenv("CLICKHOUSE_USER"),
        password=password,
        database=getenv("CLICKHOUSE_DB"),
    )
