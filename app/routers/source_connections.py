from typing import Annotated, List

from fastapi import APIRouter, HTTPException, Query
from sqlmodel import create_engine, inspect

from app.dependencies import SessionDep
from app.models.source_connection import (
    SourceConnection,
    SourceConnectionCreate,
    SourceConnectionError,
    SourceConnectionTester,
    SourceConnectionTestResult,
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

    tester = SourceConnectionTester(new_source_connection)
    test_result = tester.test_source_connection()

    if not test_result.connectivity_test:
        error = SourceConnectionError.CONNECTION_FAILED
        raise HTTPException(status_code=422, detail=error)

    if not test_result.supported_version_test:
        error = SourceConnectionError.UNSUPPORTED_VERSION
        raise HTTPException(status_code=422, detail=error)

    session.add(new_source_connection)
    session.commit()
    session.refresh(new_source_connection)

    return new_source_connection


# Test source connection
@router.post("/test")
def test_new_source_connection(
    source_connection: SourceConnectionCreate,
) -> SourceConnectionTestResult:
    """Tests new source connection."""

    new_source_connection = SourceConnection(**source_connection.model_dump())

    tester = SourceConnectionTester(new_source_connection)
    test_result = tester.test_source_connection()

    return test_result


# Test existing source connection
@router.post("/{id}/test")
def test_existing_source_connection(
    id: int, session: SessionDep
) -> SourceConnectionTestResult:
    """Tests existing source connection."""

    source_connection = session.get(SourceConnection, id)

    if not source_connection:
        error = SourceConnectionError.NOT_FOUND
        raise HTTPException(status_code=404, detail=error)

    tester = SourceConnectionTester(source_connection)
    test_result = tester.test_source_connection()

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

    source_connection.sqlmodel_update(
        source_connection_update.model_dump(exclude_unset=True)
    )

    validator = SourceConnectionValidator(source_connection)
    validator.validate_source_connection()

    tester = SourceConnectionTester(source_connection)
    test_result = tester.test_source_connection()

    if not test_result.connectivity_test:
        error = SourceConnectionError.CONNECTION_FAILED
        raise HTTPException(status_code=422, detail=error)

    if not test_result.supported_version_test:
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

    if source_connection.type == "mysql":
        engine = create_engine(
            f"mysql+pymysql://{source_connection.user}:{source_connection.password}@{source_connection.host}:{source_connection.port}/{source_connection.db}"
        )
        inspector = inspect(engine)
        return inspector.get_table_names()

    else:
        engine = create_engine(
            f"postgresql://{source_connection.user}:{source_connection.password}@{source_connection.host}:{source_connection.port}/{source_connection.db}"
        )
        inspector = inspect(engine)
        return inspector.get_table_names()


# Read source connection table schema
@router.get("/{id}/table_schema")
def read_source_connection_table_schema(id: int, session: SessionDep):
    source_connection = session.get(SourceConnection, id)

    if not source_connection:
        raise HTTPException(status_code=404, detail="Source connection not found")

    if source_connection.type == "mysql":
        engine = create_engine(
            f"mysql+pymysql://{source_connection.user}:{source_connection.password}@{source_connection.host}:{source_connection.port}/{source_connection.db}"
        )
        inspector = inspect(engine)
        return inspector.get_columns(source_connection.table_name)

    else:
        engine = create_engine(
            f"postgresql://{source_connection.user}:{source_connection.password}@{source_connection.host}:{source_connection.port}/{source_connection.db}"
        )
        inspector = inspect(engine)
        columns = inspector.get_columns(
            source_connection.table_name, source_connection.schema_name
        )
        return [
            dict(zip(data.keys(), [str(x) for x in data.values()])) for data in columns
        ]


# Read source connection table rows
@router.get("/{id}/rows")
def read_source_connection_table_rows(
    id: int, session: SessionDep, limit: Annotated[int, Query(le=100)] = 10
):
    source_connection = session.get(SourceConnection, id)
    if not source_connection:
        raise HTTPException(status_code=404, detail="Source connection not found")
    # TODO: get table rows from database
    return


# Delete source connection
@router.delete("/{id}")
def delete_source_connection(id: int, session: SessionDep) -> dict:
    source_connection = session.get(SourceConnection, id)
    if not source_connection:
        raise HTTPException(status_code=404, detail="Source connection not found")
    session.delete(source_connection)
    session.commit()
    return {"success": True}
