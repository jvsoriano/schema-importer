# from fastapi import HTTPException
# from sqlalchemy import Connection
# from sqlalchemy.exc import OperationalError
# from sqlmodel import create_engine

# from app.models.source_connection import (
#     SourceConnection,
#     SourceConnectionCreate,
#     SourceConnectionTestResult,
# )

# def validate_source_connection(
#     source_connection: SourceConnection | SourceConnectionCreate,
# ):
#     """
#     Validates source connection.
#     MySQL requires table name.
#     PostgreSQL requires schema name and table name.
#     """

#     # validate mysql required fields
#     if source_connection.type == "mysql" and not source_connection.table_name:
#         raise HTTPException(status_code=422, detail="Table name is required")

#     # validate postgresql required fields
#     if source_connection.type == "postgresql" and (
#         not source_connection.schema_name or not source_connection.table_name
#     ):
#         raise HTTPException(
#             status_code=422, detail="Schema and table name are required"
#         )


# def get_database(source_connection: SourceConnection) -> Connection | None:
#     db_type = source_connection.type
#     db_user = source_connection.user
#     db_password = source_connection.password
#     db_host = source_connection.host
#     db_port = source_connection.port
#     db = source_connection.db

#     db_url = f"{db_type}://{db_user}:{db_password}@{db_host}:{db_port}/{db}"

#     try:
#         engine = create_engine(db_url, connect_args={"check_same_thread": False})
#         session = engine.connect()
#     except OperationalError:
#         session = None

#     return session


# def get_database_version(database: Connection):
#     version_tuple = database.dialect.server_version_info
#     return ".".join(str(version) for version in version_tuple or (0,))


# def test_source_connection(
#     source_connection: SourceConnection,
# ) -> SourceConnectionTestResult:
#     test_result = SourceConnectionTestResult()

#     # test database connection
#     database = get_database(source_connection)
#     if not database:
#         return test_result

#     test_result.connectivity_test = True

#     # test database version
#     database_version = get_database_version(database)

#     # test mysql version
#     version5_5 = database_version.startswith("5.5")
#     version8_x = int(database_version[0]) >= 8

#     if source_connection.type == "mysql" and not (version5_5 or version8_x):
#         return test_result

#     test_result.supported_version_test = True

#     # test postgresql version
#     version10_x = int(database_version[0]) >= 10

#     if source_connection.type == "postgresql" and not version10_x:
#         return test_result

#     test_result.supported_version_test = True
#     test_result.success = True

#     return test_result
