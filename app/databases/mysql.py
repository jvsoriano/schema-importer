from typing import Any

from sqlalchemy.exc import ArgumentError, OperationalError
from sqlmodel import Session, create_engine, inspect

from app.databases.base import BaseDatabase


class MySQLdb(BaseDatabase):
    def connect(self, **kwargs):
        self._table_name = kwargs.get("table_name", "")
        self._schema_name = kwargs.get("schema_name", "")

        self._user = kwargs.get("user", "")
        self._password = kwargs.get("password", "")
        self._host = kwargs.get("host", "")
        self._port = kwargs.get("port", "")
        self._db = kwargs.get("db", "")

        db_url = f"mysql+pymysql://{self._user}:{self._password}@{self._host}:{self._port}/{self._db}"
        self._engine = create_engine(db_url)

        try:
            self._session = self._engine.connect()
        except OperationalError:
            self._session = None

    def table_schema(self):
        if not self._session:
            raise Exception("Can't connect to database.")

        if not self._table_name:
            return []

        inspector: Any = inspect(self._engine)
        try:
            pk_constraint = inspector.get_pk_constraint(self._table_name)
            columns = inspector.get_columns(self._table_name)
            parsed_columns = []
            for column in columns:
                parsed_columns.append(
                    {
                        **column,
                        "primary_key": column["name"]
                        in pk_constraint["constrained_columns"],
                    }
                )
            return parsed_columns
        except OperationalError:
            return []

    def table_rows(self, limit: int = 10):
        if not self._table_name:
            return []

        with Session(self._engine) as session:
            statement = f"SELECT * FROM {self._table_name} LIMIT {limit}"
            try:
                rows = session.exec(statement)  # type: ignore
                return rows
            except ArgumentError:
                return []

    def run_tests(self):
        version = self.version()
        result = {"connection_test": False, "supported_version_test": False}

        if self._session:
            result["connection_test"] = True

        if version == 5.5 or version >= 8:
            result["supported_version_test"] = True

        result["success"] = all(result.values())

        return result
