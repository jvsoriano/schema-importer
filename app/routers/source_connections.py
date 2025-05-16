from typing import Annotated, List

from fastapi import APIRouter, HTTPException, Query

from app.databases.base import DatabaseFactory
from app.dependencies import SessionDep
from app.models.source_connection import (
    SourceConnection,
    SourceConnectionCreate,
    SourceConnectionError,
    SourceConnectionUpdate,
    SourceConnectionValidator,
)

router = APIRouter(prefix="/source-connection", tags=["Source Connection"])


@router.post("/")
def create_source_connection(
    source_connection: SourceConnectionCreate, session: SessionDep
) -> SourceConnection:
    """Creates new source connection."""

    new_source_connection = SourceConnection(**source_connection.model_dump())

    database_factory = DatabaseFactory()
    database = database_factory.get_database(new_source_connection.type)
    database.connect(**new_source_connection.model_dump(exclude_unset=True))

    test_result = database.run_tests()

    if not test_result["connection_test"]:
        error = SourceConnectionError.CONNECTION_FAILED
        raise HTTPException(status_code=422, detail=error)

    if not test_result["supported_version_test"]:
        error = SourceConnectionError.UNSUPPORTED_VERSION
        raise HTTPException(status_code=422, detail=error)

    database.close()

    session.add(new_source_connection)
    session.commit()
    session.refresh(new_source_connection)

    return new_source_connection


# Test source connection
@router.post("/test")
def test_new_source_connection(
    source_connection: SourceConnectionCreate,
) -> dict[str, bool]:
    """Tests new source connection."""

    new_source_connection = SourceConnection(**source_connection.model_dump())

    database_factory = DatabaseFactory()
    database = database_factory.get_database(new_source_connection.type)
    database.connect(**new_source_connection.model_dump(exclude_unset=True))

    test_result = database.run_tests()

    return test_result


# Test existing source connection
@router.post("/{id}/test")
def test_existing_source_connection(id: int, session: SessionDep) -> dict[str, bool]:
    """Tests existing source connection."""

    source_connection = session.get(SourceConnection, id)

    if not source_connection:
        error = SourceConnectionError.NOT_FOUND
        raise HTTPException(status_code=404, detail=error)

    database_factory = DatabaseFactory()
    database = database_factory.get_database(source_connection.type)
    database.connect(**source_connection.model_dump())

    test_result = database.run_tests()

    return test_result


# Update source connection
@router.patch("/{id}")
def update_source_connection(
    id: int, source_connection_update: SourceConnectionUpdate, session: SessionDep
) -> SourceConnection:
    """Updates source connection."""

    source_connection = session.get(SourceConnection, id)

    if not source_connection:
        error = SourceConnectionError.NOT_FOUND
        raise HTTPException(status_code=404, detail=error)

    source_connection_update_data = source_connection_update.model_dump(
        exclude_unset=True
    )

    source_connection.sqlmodel_update(source_connection_update_data)

    validator = SourceConnectionValidator(source_connection)
    validator.validate_source_connection()

    database_factory = DatabaseFactory()
    database = database_factory.get_database(source_connection.type)
    database.connect(**source_connection.model_dump())

    test_result = database.run_tests()

    if not test_result["connection_test"]:
        error = SourceConnectionError.CONNECTION_FAILED
        raise HTTPException(status_code=422, detail=error)

    if not test_result["supported_version_test"]:
        error = SourceConnectionError.UNSUPPORTED_VERSION
        raise HTTPException(status_code=422, detail=error)

    session.add(source_connection)
    session.commit()
    session.refresh(source_connection)

    return source_connection


# Read source connection list of available tables
@router.get("/{id}/tables")
def read_source_connection_tables(id: int, session: SessionDep) -> List[str]:
    source_connection = session.get(SourceConnection, id)

    if not source_connection:
        error = SourceConnectionError.NOT_FOUND
        raise HTTPException(status_code=404, detail=error)

    database_factory = DatabaseFactory()
    database = database_factory.get_database(source_connection.type)
    database.connect(**source_connection.model_dump())

    return database.tables()


# Read source connection table schema
@router.get("/{id}/table-schema")
def read_source_connection_table_schema(id: int, session: SessionDep):
    source_connection = session.get(SourceConnection, id)

    if not source_connection:
        raise HTTPException(status_code=404, detail="Source connection not found")

    database_factory = DatabaseFactory()
    database = database_factory.get_database(source_connection.type)
    database.connect(**source_connection.model_dump())

    return database.table_schema(
        source_connection.table_name, source_connection.schema_name
    )


# Read source connection table rows
@router.get("/{id}/rows")
def read_source_connection_table_rows(
    id: int, session: SessionDep, limit: Annotated[int, Query(le=100)] = 10
):
    source_connection = session.get(SourceConnection, id)

    if not source_connection:
        raise HTTPException(status_code=404, detail="Source connection not found")

    database_factory = DatabaseFactory()
    database = database_factory.get_database(source_connection.type)
    database.connect(**source_connection.model_dump())

    rows = database.table_rows(
        source_connection.table_name, source_connection.schema_name, limit
    )
    return rows


# Delete source connection
@router.delete("/{id}")
def delete_source_connection(id: int, session: SessionDep) -> dict:
    """Deletes source connection from database."""

    source_connection = session.get(SourceConnection, id)

    if not source_connection:
        raise HTTPException(status_code=404, detail="Source connection not found")

    session.delete(source_connection)
    session.commit()

    return {"success": True}
