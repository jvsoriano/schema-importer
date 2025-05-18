from typing import Any

from sqlmodel import create_engine, inspect, text


class MySQLdb:
    def __init__(self, source_connection: dict[str, Any]) -> None:
        self.__table = source_connection["table_name"]

        user = source_connection["user"]
        password = source_connection["password"]
        host = source_connection["host"]
        port = source_connection["port"]
        db = source_connection["db"]
        url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"

        self.__engine = create_engine(url)
        self.__inspector = inspect(self.__engine)

    def get_table_names(self):
        """Returns available table names."""
        return self.__inspector.get_table_names()

    def get_table_schema(self):
        """Returns table information."""
        pk_constraint = self.__inspector.get_pk_constraint(self.__table)
        columns = self.__inspector.get_columns(self.__table)
        return [
            {
                **dict(zip(column.keys(), [str(val) for val in column.values()])),
                "primary_key": str(
                    column["name"] in pk_constraint["constrained_columns"]
                ),
            }
            for column in columns
        ]

    def get_table_rows(self, limit: int = 10):
        """Returns table rows."""
        with self.__engine.connect() as session:
            columns = self.__inspector.get_columns(self.__table)
            column_names = [column["name"] for column in columns]
            statement = f"SELECT * FROM {self.__table} LIMIT {limit}"
            rows = session.execute(text(statement)).all()
            return [dict(zip(column_names, row)) for row in rows]
