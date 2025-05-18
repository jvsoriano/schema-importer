from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from app.databases import DatabaseFactory
from app.dependencies import SessionDep
from app.models.source_connection import (
    SourceConnection,
    SourceConnectionCreate,
    SourceConnectionPublic,
    SourceConnectionUpdate,
)
from app.testers import SourceConnectionTester
from app.validators import SourceConnectionValidator

NOT_FOUND_ERROR = "Source connection not found."

router = APIRouter(prefix="/source-connection", tags=["Source Connection"])


@router.post("/")
def create_source_connection(
    source_connection: SourceConnectionCreate, session: SessionDep
) -> SourceConnectionPublic:
    """Creates new source connection."""

    source_connection_dict = source_connection.model_dump()
    source_connection_validator = SourceConnectionValidator(source_connection_dict)
    source_connection_validator.validate()

    source_connection_tester = SourceConnectionTester(
        source_connection_dict, raise_exceptions=True
    )
    source_connection_tester.test()

    new_source_connection = SourceConnection(**source_connection_dict)

    session.add(new_source_connection)
    session.commit()
    session.refresh(new_source_connection)

    return SourceConnectionPublic(**new_source_connection.model_dump())


@router.post("/test")
def test_new_source_connection(
    source_connection: SourceConnectionCreate,
) -> dict[str, bool]:
    """Tests source connection."""

    source_connection_dict = source_connection.model_dump()
    source_connection_tester = SourceConnectionTester(source_connection_dict)
    source_connection_tester.test()

    return source_connection_tester.test_result()


@router.post("/{id}/test")
def test_existing_source_connection(id: int, session: SessionDep) -> dict[str, bool]:
    """Tests existing source connection."""

    source_connection = session.get(SourceConnection, id)

    if not source_connection:
        raise HTTPException(status_code=404, detail=NOT_FOUND_ERROR)

    source_connection_dict = source_connection.model_dump()
    source_connection_tester = SourceConnectionTester(source_connection_dict)
    source_connection_tester.test()

    return source_connection_tester.test_result()


@router.patch("/{id}")
def update_source_connection(
    id: int, source_connection_update: SourceConnectionUpdate, session: SessionDep
) -> SourceConnectionPublic:
    """Updates source connection."""

    source_connection = session.get(SourceConnection, id)

    if not source_connection:
        raise HTTPException(status_code=404, detail=NOT_FOUND_ERROR)

    source_connection_update_dict = source_connection_update.model_dump(
        exclude_unset=True
    )
    source_connection.sqlmodel_update(source_connection_update_dict)

    source_connection_dict = source_connection.model_dump()
    source_connection_validator = SourceConnectionValidator(source_connection_dict)
    source_connection_validator.validate()

    source_connection_tester = SourceConnectionTester(
        source_connection_dict, raise_exceptions=True
    )
    source_connection_tester.test()

    session.add(source_connection)
    session.commit()
    session.refresh(source_connection)

    return SourceConnectionPublic(**source_connection.model_dump())


@router.get("/{id}/tables")
def read_source_connection_tables(id: int, session: SessionDep):
    source_connection = session.get(SourceConnection, id)

    if not source_connection:
        raise HTTPException(status_code=404, detail=NOT_FOUND_ERROR)

    source_connection_dict = source_connection.model_dump()
    database_factory = DatabaseFactory(source_connection_dict)
    database = database_factory.get_database()

    return database.get_table_names()


@router.get("/{id}/table-schema")
def read_source_connection_table_schema(id: int, session: SessionDep):
    source_connection = session.get(SourceConnection, id)

    if not source_connection:
        raise HTTPException(status_code=404, detail=NOT_FOUND_ERROR)

    source_connection_dict = source_connection.model_dump()
    database_factory = DatabaseFactory(source_connection_dict)
    database = database_factory.get_database()

    return database.get_table_schema()


@router.get("/{id}/rows")
def read_source_connection_table_rows(
    id: int, session: SessionDep, limit: Annotated[int, Query(le=100)] = 10
):
    source_connection = session.get(SourceConnection, id)

    if not source_connection:
        raise HTTPException(status_code=404, detail=NOT_FOUND_ERROR)

    source_connection_dict = source_connection.model_dump()
    database_factory = DatabaseFactory(source_connection_dict)
    database = database_factory.get_database()

    return database.get_table_rows(limit)


@router.delete("/{id}")
def delete_source_connection(id: int, session: SessionDep) -> dict:
    """Deletes source connection from database."""

    source_connection = session.get(SourceConnection, id)

    if not source_connection:
        raise HTTPException(status_code=404, detail=NOT_FOUND_ERROR)

    session.delete(source_connection)
    session.commit()

    return {"success": True}
