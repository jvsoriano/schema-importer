from enum import Enum
from typing import Any

from fastapi import HTTPException


class Error(str, Enum):
    TABLE_REQUIRED_ERROR = "Table name is required."
    SCHEMA_REQUIRED_ERROR = "Schema name is required."


class SourceConnectionValidator:
    """
    Validator class for source connection.
    """

    def __init__(self, source_connection: dict[str, Any]) -> None:
        self._type = source_connection.get("type")
        self._table = source_connection.get("table_name")
        self._schema = source_connection.get("schema_name")

    def _validate_mysql_connection(self):
        if not self._table:
            raise HTTPException(status_code=422, detail=Error.TABLE_REQUIRED_ERROR)

    def _validate_postgresql_connection(self):
        self._validate_mysql_connection()
        if not self._schema:
            raise HTTPException(status_code=422, detail=Error.SCHEMA_REQUIRED_ERROR)

    def validate(self):
        """
        Validates source connection.
        MySQL requires table name.
        PostgreSQL requires schema name and table name.
        """
        if self._type == "mysql":
            self._validate_mysql_connection()
        else:
            self._validate_postgresql_connection()
