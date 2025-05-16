from sqlalchemy.exc import ArgumentError, NoSuchTableError, OperationalError
from sqlmodel import Session, SQLModel, create_engine, inspect


class PostgreSQLdb:
    def initialize(self):
        SQLModel.metadata.create_all(self._engine)

    def connect(self, **kwargs):
        user = kwargs.get("user", "")
        password = kwargs.get("password", "")
        host = kwargs.get("host", "")
        port = kwargs.get("port", "")
        db = kwargs.get("db", "")

        db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        self._engine = create_engine(db_url)

        try:
            self._session = self._engine.connect()
        except OperationalError:
            self._session = None

    def close(self):
        if self._session:
            self._session.close()

    def tables(self):
        if not self._session:
            raise Exception("Can't connect to database.")

        inspector = inspect(self._engine)
        try:
            return inspector.get_table_names()
        except AttributeError:
            return []

    def table_schema(
        self, table_name: str | None = None, schema_name: str | None = None
    ):
        if not self._session:
            raise Exception("Can't connect to database.")

        if not table_name:
            return []

        inspector = inspect(self._engine)
        try:
            columns = inspector.get_columns(table_name, schema_name)
            converted_columns = [
                dict(zip(column.keys(), [str(val) for val in column.values()]))
                for column in columns
            ]
            return converted_columns
        except NoSuchTableError:
            return []

    def table_rows(
        self,
        table_name: str | None = None,
        schema_name: str | None = None,
        limit: int = 10,
    ):
        if not table_name:
            return []

        with Session(self._engine) as session:
            statement = f"SELECT * FROM {schema_name}.{table_name} LIMIT {limit}"
            try:
                rows = session.exec(statement)  # type: ignore
                return rows
            except ArgumentError:
                return []

    def version(self):
        if not self._session:
            raise Exception("Can't connect to database.")

        version_fallback = (
            0,
            0,
        )
        version_tuple = self._session.dialect.server_version_info or version_fallback
        version_major_minor = version_tuple[0:2]
        version_string = ".".join([str(version) for version in version_major_minor])
        return float(version_string)

    def run_tests(self):
        version = self.version()
        result = {"connection_test": False, "supported_version_test": False}

        if self._session:
            result["connection_test"] = True

        if version >= 10:
            result["supported_version_test"] = True

        result["success"] = all(result.values())

        return result
