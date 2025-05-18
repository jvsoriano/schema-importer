from typing import Any, Protocol, Sequence

from app.databases.mysql import MySQLdb
from app.databases.postgres import PostgreSQLdb


class Database(Protocol):
    def get_table_names(self) -> list[str]: ...

    def get_table_schema(self) -> list[dict[str, Any]]: ...

    def get_table_rows(self, limit: int = 10) -> Sequence[Any]: ...


class DatabaseFactory:
    def __init__(self, source_connection: dict[str, Any]) -> None:
        self.__source_connection = source_connection

    def get_database(self) -> Database:
        if self.__source_connection["type"] == "mysql":
            return MySQLdb(self.__source_connection)
        else:
            return PostgreSQLdb(self.__source_connection)
