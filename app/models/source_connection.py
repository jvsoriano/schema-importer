from enum import StrEnum
from typing import Literal, Self

from fastapi import HTTPException
from pydantic import model_validator
from sqlmodel import Field, SQLModel


class SourceConnectionError(StrEnum):
    NOT_FOUND = "Source connection not found."

    TABLE_REQUIRED = "Table name is required."
    SCHEMA_REQUIRED = "Schema name is required."

    CONNECTION_FAILED = "Connectivity test failed."
    UNSUPPORTED_VERSION = "Database version not supported."


class SourceConnectionPublic(SQLModel):
    """
    Public data model of source connection.
    Should not display password in public.
    """

    id: int
    schema_name: str | None = None  # for postgresql only
    table_name: str

    user: str
    host: str
    port: int
    db: str


class SourceConnectionBase(SQLModel):
    """Base data model of source connection."""

    schema_name: str | None = None  # for postgresql only
    table_name: str | None = None

    # connection requirements
    user: str
    password: str
    host: str
    port: int
    db: str


class SourceConnection(SourceConnectionBase, table=True):
    """
    Table model of source connection.
    Inherits source connection base.
    """

    id: int | None = Field(default=None, primary_key=True)
    type: str  # mysql or postgresql


class SourceConnectionCreate(SourceConnectionBase):
    """
    Data model for creating source connection.
    Only accepts 'mysql' or 'postgresql' as type.
    """

    type: Literal["mysql", "postgresql"]

    @model_validator(mode="after")
    def validate_create(self) -> Self:
        validator = SourceConnectionValidator(self)
        validator.validate_source_connection()
        return self


class SourceConnectionUpdate(SQLModel):
    """
    Data model for updating source connection.
    Does not allow updating of source connection type.
    """

    schema_name: str | None = None  # for postgresql only
    table_name: str | None = None

    # connection requirements
    user: str | None = None
    password: str | None = None
    host: str | None = None
    port: int | None = None
    db: str | None = None


class SourceConnectionValidator:
    """Validator class for source connection."""

    def __init__(self, conn: SourceConnection | SourceConnectionCreate) -> None:
        self._conn = conn

    def validate_source_connection(self):
        """
        Validates source connection.
        MySQL requires table name.
        PostgreSQL requires schema name and table name.
        """

        if self._conn.type == "mysql" and not self._conn.table_name:
            error = SourceConnectionError.TABLE_REQUIRED
            raise HTTPException(status_code=422, detail=error)

        if self._conn.type == "postgresql" and not self._conn.table_name:
            error = SourceConnectionError.TABLE_REQUIRED
            raise HTTPException(status_code=422, detail=error)

        if self._conn.type == "postgresql" and not self._conn.schema_name:
            error = SourceConnectionError.SCHEMA_REQUIRED
            raise HTTPException(status_code=422, detail=error)
