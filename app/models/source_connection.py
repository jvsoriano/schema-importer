from typing import Literal, Self

from fastapi import HTTPException
from pydantic import model_validator
from sqlalchemy import Connection
from sqlalchemy.exc import OperationalError
from sqlmodel import Field, SQLModel, create_engine


class SourceConnectionBase(SQLModel):
    """Base data model of source connection."""

    schema_name: str | None  # for postgresql only
    table_name: str

    # connection requirements
    user: str
    password: str
    host: str
    port: int
    db: str


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
        # # validate mysql required fields
        # if self.type == "mysql" and not self.table_name:
        #     raise HTTPException(status_code=422, detail="Table name is required")

        # # validate postgresql required fields
        # if self.type == "postgresql" and (not self.schema_name or not self.table_name):
        #     raise HTTPException(
        #         status_code=422, detail="Schema and table name are required"
        #     )

        # validate database connection
        # database = get_database(self)
        # if not database:
        #     raise HTTPException(status_code=422, detail="Failed to connect.")

        # # validate database version
        # database_version = get_database_version(database)
        # version5_5 = database_version.startswith("5.5")
        # version8_x = int(database_version[0]) >= 8
        # version10_x = int(database_version[0]) >= 10
        # if self.type == "mysql" and not (version5_5 or version8_x):
        #     raise HTTPException(
        #         status_code=422, detail="Database version not supported."
        #     )
        # if self.type == "postgresql" and not version10_x:
        #     raise HTTPException(
        #         status_code=422, detail="Database version not supported."
        #     )
        return self


class SourceConnection(SourceConnectionBase, table=True):
    """Table model of source connection."""

    id: int | None = Field(default=None, primary_key=True)
    type: str  # mysql or postgresql


# Create source connection test result schema
class SourceConnectionTestResult(SQLModel):
    """Data model for source connection test result"""

    connectivity_test: bool = False
    supported_version_test: bool = False
    success: bool = False  # overall test result


class SourceConnectionUpdate(SQLModel):
    """
    Data model for updating source connection.
    Does not allow updating of source connection type.
    """

    schema_name: str | None  # for postgresql only
    table_name: str | None

    # connection requirements
    user: str | None
    password: str | None
    host: str | None
    port: int | None
    db: str | None


class SourceConnectionValidator:
    def __init__(self, conn: SourceConnection | SourceConnectionCreate) -> None:
        self._conn = conn

    def validate_source_connection(self):
        """
        Validates source connection.
        MySQL requires table name.
        PostgreSQL requires schema name and table name.
        """

        # validate mysql required fields
        if self._conn.type == "mysql" and not self._conn.table_name:
            raise HTTPException(status_code=422, detail="Table name is required")

        # validate postgresql required fields
        if self._conn.type == "postgresql" and (
            not self._conn.schema_name or not self._conn.table_name
        ):
            raise HTTPException(
                status_code=422, detail="Schema and table name are required"
            )


class SourceConnectionTester:
    def __init__(self, conn: SourceConnection) -> None:
        self._conn = conn

    def _get_database(self) -> Connection | None:
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

    def _get_database_version(self, database: Connection):
        version_tuple = database.dialect.server_version_info
        return ".".join(str(version) for version in version_tuple or (0,))

    def test_source_connection(self) -> SourceConnectionTestResult:
        test_result = SourceConnectionTestResult()

        # test database connection
        database = self._get_database()
        if not database:
            return test_result

        test_result.connectivity_test = True

        # test database version
        database_version = self._get_database_version(database)

        # test mysql version
        version5_5 = database_version.startswith("5.5")
        version8_x = int(database_version[0]) >= 8

        if self._conn.type == "mysql" and not (version5_5 or version8_x):
            return test_result

        test_result.supported_version_test = True

        # test postgresql version
        version10_x = int(database_version[0]) >= 10

        if self._conn.type == "postgresql" and not version10_x:
            return test_result

        test_result.supported_version_test = True
        test_result.success = True

        return test_result
