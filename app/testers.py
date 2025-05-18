from enum import Enum
from os import getenv
from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel
from sqlalchemy.exc import OperationalError
from sqlalchemy_utils import database_exists
from sqlmodel import create_engine, inspect, text


class Error(str, Enum):
    INVALID_CREDENTIALS_ERROR = "Database server or credentials is invalid."
    INVALID_DATABASE_ERROR = "Database does not exist."
    INVALID_TABLE_ERROR = "Database table does not exist."
    INVALID_SCHEMA_ERROR = "Database schema does not exist."
    SUPPORTED_VERSION_ERROR = "Database version is not supported."
    USER_CREATE_SCHEMA_PRIVILEGE_ERROR = "User has no create privilege in schema."
    USER_CREATE_DATABASE_PRIVILEGE_ERROR = "User has no create privilege in database."


class TestResult(BaseModel):
    valid_credentials: bool | None = None
    valid_database: bool | None = None
    valid_table: bool | None = None
    valid_schema: bool | None = None
    supported_version: bool | None = None
    user_create_schema_privilege: bool | None = None
    user_create_database_privilege: bool | None = None


class SourceConnectionTester:
    def __init__(
        self, source_connection: dict[str, Any], raise_exceptions: bool = False
    ) -> None:
        self.__type = source_connection["type"]
        self.__table = source_connection["table_name"]
        self.__schema = source_connection["schema_name"]

        self.__user = source_connection["user"]
        self.__password = source_connection["password"]
        self.__host = source_connection["host"]
        self.__port = source_connection["port"]
        self.__db = source_connection["db"]

        self.__raise_exceptions = raise_exceptions

        self.__credentials_mapping = {
            "mysql": {
                "user": getenv("MYSQL_USER"),
                "password": getenv("MYSQL_PASSWORD"),
            },
            "postgresql": {
                "user": getenv("POSTGRES_USER"),
                "password": getenv("POSTGRES_PASSWORD"),
            },
        }

        self.__result = TestResult(
            valid_credentials=False,
            valid_database=False,
            valid_table=False,
            supported_version=False,
        )

        if self.__type == "postgresql":
            self.__result.valid_schema = False
            self.__result.user_create_schema_privilege = False
            self.__result.user_create_database_privilege = False

    def __url(self, user: str | None = None, password: str | None = None):
        connection_mapping = {"mysql": "mysql+pymysql", "postgresql": "postgresql"}

        return f"{connection_mapping[self.__type]}://{user or self.__user}:{password or self.__password}@{self.__host}:{self.__port}/{self.__db}"

    def __version(self, session):
        version_fallback = [0, 0]
        version_tuple = session.dialect.server_version_info or version_fallback
        version_major_minor = version_tuple[0:2]
        version_string = ".".join([str(version) for version in version_major_minor])

        return float(version_string)

    def __test_database(self, engine):
        try:
            connection = engine.connect()
            connection.close()
            self.__result.valid_credentials = True
        except OperationalError:
            self.__result.valid_credentials = False

        if self.__raise_exceptions and not self.__result.valid_credentials:
            raise HTTPException(status_code=422, detail=Error.INVALID_CREDENTIALS_ERROR)

        try:
            self.__result.valid_database = database_exists(engine.url)
        except OperationalError:
            self.__result.valid_database = False

        if self.__raise_exceptions and not self.__result.valid_database:
            raise HTTPException(status_code=422, detail=Error.INVALID_DATABASE_ERROR)

    def __test_table(self, inspector):
        self.__result.valid_table = self.__table in inspector.get_table_names()

        if self.__raise_exceptions and not self.__result.valid_table:
            raise HTTPException(status_code=422, detail=Error.INVALID_TABLE_ERROR)

    def __test_schema(self, inspector):
        self.__result.valid_schema = self.__schema in inspector.get_schema_names()

        if self.__raise_exceptions and not self.__result.valid_schema:
            raise HTTPException(status_code=422, detail=Error.INVALID_SCHEMA_ERROR)

    def __test_version(self, session):
        current_version = self.__version(session)
        supported_versions = {"mysql": [5.5, 8], "postgresql": [10]}
        self.__result.supported_version = current_version in supported_versions[
            self.__type
        ] or current_version > max(supported_versions[self.__type])

        if self.__raise_exceptions and not self.__result.supported_version:
            raise HTTPException(status_code=422, detail=Error.SUPPORTED_VERSION_ERROR)

    def __test_user_schema_privilege(self):
        user = self.__credentials_mapping[self.__type]["user"]
        password = self.__credentials_mapping[self.__type]["password"]
        engine = create_engine(self.__url(user, password))
        with engine.connect() as session:
            statement = f"SELECT pg_catalog.has_schema_privilege('{self.__user}', '{self.__schema}', 'CREATE');"
            schema_privilege = session.execute(text(statement))
            for result in schema_privilege:
                self.__result.user_create_schema_privilege = True in result

        if self.__raise_exceptions and not self.__result.user_create_schema_privilege:
            raise HTTPException(
                status_code=422, detail=Error.USER_CREATE_SCHEMA_PRIVILEGE_ERROR
            )

    def __test_user_database_privilege(self):
        user = self.__credentials_mapping[self.__type]["user"]
        password = self.__credentials_mapping[self.__type]["password"]
        engine = create_engine(self.__url(user, password))
        with engine.connect() as session:
            statement = f"SELECT pg_catalog.has_database_privilege('{self.__user}', '{self.__db}', 'CREATE');"
            db_privilege = session.execute(text(statement))
            for result in db_privilege:
                self.__result.user_create_database_privilege = True in result

        if self.__raise_exceptions and not self.__result.user_create_database_privilege:
            raise HTTPException(
                status_code=422, detail=Error.USER_CREATE_DATABASE_PRIVILEGE_ERROR
            )

    def test(self):
        engine = create_engine(self.__url())
        self.__test_database(engine)

        if not self.__result.valid_database:
            return

        inspector = inspect(engine)
        self.__test_table(inspector)

        with engine.connect() as session:
            self.__test_version(session)

            if self.__type == "postgresql":
                self.__test_schema(inspector)
                self.__test_user_schema_privilege()
                self.__test_user_database_privilege()

    def test_result(self):
        result_dict = self.__result.model_dump(exclude_unset=True)
        result_dict["success"] = all(result_dict.values())

        return result_dict
