from typing import Any

from sqlalchemy.exc import ArgumentError, NoSuchTableError, OperationalError
from sqlmodel import Session, create_engine, inspect, text

from app.databases.base import BaseDatabase


class PostgreSQLdb(BaseDatabase):
    def connect(self, **kwargs):
        self._table_name = kwargs.get("table_name", "")
        self._schema_name = kwargs.get("schema_name", "")

        self._user = kwargs.get("user", "")
        self._password = kwargs.get("password", "")
        self._host = kwargs.get("host", "")
        self._port = kwargs.get("port", "")
        self._db = kwargs.get("db", "")

        db_url = f"postgresql://{self._user}:{self._password}@{self._host}:{self._port}/{self._db}"
        self._engine = create_engine(db_url)  # noqa: F821

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
            pk_constraint = inspector.get_pk_constraint(
                self._table_name, self._schema_name
            )
            columns = inspector.get_columns(self._table_name, self._schema_name)
            parsed_columns = []
            for column in columns:
                converted_column = dict(
                    zip(column.keys(), [str(val) for val in column.values()])
                )
                parsed_columns.append(
                    {
                        **converted_column,
                        "primary_key": column["name"]
                        in pk_constraint["constrained_columns"],
                    }
                )
            return parsed_columns
        except NoSuchTableError:
            return []

    def table_rows(self, limit: int = 10):
        if not self._table_name:
            return []

        with Session(self._engine) as session:
            statement = (
                f"SELECT * FROM {self._schema_name}.{self._table_name} LIMIT {limit}"
            )
            try:
                rows = session.exec(statement)  # type: ignore
                return rows
            except ArgumentError:
                return []

    def run_tests(self):
        version = self.version()
        result = {
            "connection_test": False,
            "supported_version_test": False,
            "has_schema_privilege": False,
            "has_table_privilege": False,
        }

        if self._session:
            result["connection_test"] = True

        if version >= 10:
            result["supported_version_test"] = True

        with Session(self._engine) as session:
            # check if user has schema privilege
            if self._schema_name:
                statement = f"SELECT pg_catalog.has_schema_privilege('{self._user}', '{self._schema_name}', 'CREATE');"
                schema_privilege = session.exec(text(statement))  # type: ignore
                result["has_schema_privilege"] = True in next(schema_privilege)

            # check if user has table privilege
            if self._table_name:
                statement = f"SELECT privilege_type FROM information_schema.role_table_grants WHERE grantee='{self._user}' AND table_name='{self._table_name}' AND privilege_type='INSERT';"
                table_privilege = session.exec(text(statement))  # type: ignore
                result["has_table_privilege"] = "INSERT" in next(table_privilege)

        result["success"] = all(result.values())

        return result
