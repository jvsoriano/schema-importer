from typing import Any, List, Protocol

from app.databases.mysql import MySQLdb
from app.databases.postgres import PostgreSQLdb


class Database(Protocol):
    def initialize(self): ...

    def connect(self, **kwargs: dict[str, str | int]): ...

    def close(self) -> None: ...

    def tables(self) -> List[str]: ...

    def table_schema(self) -> List[Any]: ...

    def table_rows(
        self,
        limit: int = 10,
    ) -> List[Any]: ...

    def version(self) -> float: ...

    def run_tests(self) -> dict[str, bool]: ...


class DatabaseFactory:
    def get_database(self, db_type: str) -> Database:
        if db_type == "mysql":
            return MySQLdb()
        elif db_type == "postgresql":
            return PostgreSQLdb()
        else:
            raise ValueError("Invalid database type.")
