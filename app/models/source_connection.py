from enum import StrEnum
from typing import Literal, Self

from fastapi import HTTPException
from pydantic import BaseModel, model_validator
from sqlalchemy import Connection
from sqlalchemy.exc import OperationalError
from sqlmodel import Field, SQLModel, create_engine


class SourceConnectionError(StrEnum):
    NOT_FOUND = "Source connection not found."

    TABLE_REQUIRED = "Table name is required."
    SCHEMA_REQUIRED = "Schema name is required."

    CONNECTION_FAILED = "Connectivity test failed."
    UNSUPPORTED_VERSION = "Database version not supported."


class SourceConnectionBase(SQLModel):
    """Base data model of source connection."""

    schema_name: str | None = None  # for postgresql only
    table_name: str

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

        # Already handled by SQLModel
        # if self._conn.type == "mysql" and not self._conn.table_name:
        #     error = SourceConnectionError.TABLE_REQUIRED
        #     raise HTTPException(status_code=422, detail=error)

        if self._conn.type == "postgresql" and not self._conn.table_name:
            error = SourceConnectionError.TABLE_REQUIRED
            raise HTTPException(status_code=422, detail=error)

        if self._conn.type == "postgresql" and not self._conn.schema_name:
            error = SourceConnectionError.SCHEMA_REQUIRED
            raise HTTPException(status_code=422, detail=error)


class SourceConnectionTestResult(BaseModel):
    """Test result schema for source connection tester."""

    connectivity_test: bool
    supported_version_test: bool
    success: bool  # overall test result


class SourceConnectionTester:
    """Tester class for source connection."""

    def __init__(self, conn: SourceConnection) -> None:
        self._conn = conn

    def _get_database(self) -> Connection | None:
        """Returns database connection based on type."""

        dialects = {"mysql": "mysql+pymysql", "postgresql": "postgresql"}

        db_type = self._conn.type
        db_user = self._conn.user
        db_password = self._conn.password
        db_host = self._conn.host
        db_port = self._conn.port
        db = self._conn.db

        db_url = (
            f"{dialects[db_type]}://{db_user}:{db_password}@{db_host}:{db_port}/{db}"
        )

        try:
            engine = create_engine(db_url)
            session = engine.connect()
        except OperationalError:
            session = None

        return session

    def _get_database_version(self, database: Connection) -> dict:
        """Returns database version."""

        version_tuple = database.dialect.server_version_info
        version_string = ".".join(str(version) for version in version_tuple or (0,))
        return {
            "version_tuple": version_tuple,
            "version_string": version_string,
        }

    def get_database(self):
        return self._get_database()

    def test_source_connection(self) -> SourceConnectionTestResult:
        """
        Executes series of tests for source connection.
        1. Tests if can connect to database.
        2. Tests if database version is supported.

        MySQL connection only supports version 5.5, 8 and above.
        PostgreSQL connection only suports version 10 and above.
        """

        test_result = SourceConnectionTestResult(
            connectivity_test=False, supported_version_test=False, success=False
        )
        database = self._get_database()

        if database:
            test_result.connectivity_test = True

            database_version = self._get_database_version(database)
            database_version5_5 = database_version["version_string"].startswith("5.5")
            database_version8_x = database_version["version_tuple"][0] >= 8
            database_version10_x = database_version["version_tuple"][0] >= 10

            if self._conn.type == "mysql" and (
                database_version5_5 or database_version8_x
            ):
                test_result.supported_version_test = True
            elif self._conn.type == "postgresql" and database_version10_x:
                test_result.supported_version_test = True

        test_result.success = all(
            (
                test_result.connectivity_test,
                test_result.supported_version_test,
            )
        )

        return test_result
