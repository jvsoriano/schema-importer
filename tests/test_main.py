import copy

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, StaticPool, create_engine

from app.dependencies import get_session
from app.main import app
from app.models.source_connection import SourceConnectionError

client = TestClient(app)

sqlite_url = "sqlite:///:memory:"

connect_args = {"check_same_thread": False}
engine = create_engine(sqlite_url, connect_args=connect_args, poolclass=StaticPool)


def get_session_replacement():
    with Session(engine) as session:
        yield session


app.dependency_overrides[get_session] = get_session_replacement


@pytest.fixture(scope="session")
def setup_and_teardown():
    SQLModel.metadata.create_all(engine)
    yield
    SQLModel.metadata.drop_all(engine)


def test_create_source_connection_mysql_support(setup_and_teardown):
    url = "/source-connection/"
    data = {
        "type": "mysql",
        "table_name": "test_table",
        "user": "root",
        "password": "example",
        "host": "localhost",
        "port": 3306,
        "db": "",
    }
    response = client.post(url, json=data)
    assert response.status_code == 200, response.text
    assert "id" in response.json()

    del data["table_name"]
    response = client.post(url, json=data)
    assert response.status_code == 422, response.text


def test_create_source_connection_postgresql_support(setup_and_teardown):
    url = "/source-connection/"
    data = {
        "type": "postgresql",
        "schema_name": "test_schema",
        "table_name": "test_table",
        "user": "postgres",
        "password": "example",
        "host": "localhost",
        "port": 5432,
        "db": "",
    }
    response = client.post(url, json=data)
    assert response.status_code == 200, response.text
    assert "id" in response.json()

    del data["schema_name"]
    response = client.post(url, json=data)
    response_json = response.json()
    assert response.status_code == 422, response.text
    assert response_json.get("detail") == SourceConnectionError.SCHEMA_REQUIRED

    del data["table_name"]
    response = client.post(url, json=data)
    response_json = response.json()
    assert response.status_code == 422, response.text


def test_test_new_mysql_source_connection(setup_and_teardown):
    url = "/source-connection/test"
    data = {
        "type": "mysql",
        "table_name": "test_table",
        "user": "root",
        "password": "example",
        "host": "localhost",
        "port": 3306,
        "db": "",
    }
    response = client.post(url, json=data)
    response_json = response.json()

    assert response.status_code == 200, response.text
    assert response_json.get("connection_test")
    assert response_json.get("supported_version_test")
    assert response_json.get("success")


def test_test_new_postgresql_source_connection(setup_and_teardown):
    url = "/source-connection/test"
    data = {
        "type": "postgresql",
        "schema_name": "test_schema",
        "table_name": "test_table",
        "user": "postgres",
        "password": "example",
        "host": "localhost",
        "port": 5432,
        "db": "",
    }
    response = client.post(url, json=data)
    response_json = response.json()

    assert response.status_code == 200, response.text
    assert response_json.get("connection_test")
    assert response_json.get("supported_version_test")
    assert response_json.get("success")


def test_test_existing_mysql_source_connection(setup_and_teardown):
    create_url = "/source-connection/"
    data = {
        "type": "mysql",
        "table_name": "test_table",
        "user": "root",
        "password": "example",
        "host": "localhost",
        "port": 3306,
        "db": "",
    }
    response = client.post(create_url, json=data)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert "id" in response_json

    test_url = f"/source-connection/{response_json.get('id')}/test"
    response = client.post(test_url)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert response_json.get("connection_test")
    assert response_json.get("supported_version_test")
    assert response_json.get("success")


def test_test_existing_postgresql_source_connection(setup_and_teardown):
    create_url = "/source-connection/"
    data = {
        "type": "postgresql",
        "schema_name": "test_schema",
        "table_name": "test_table",
        "user": "postgres",
        "password": "example",
        "host": "localhost",
        "port": 5432,
        "db": "",
    }
    response = client.post(create_url, json=data)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert "id" in response_json

    test_url = f"/source-connection/{response_json.get('id')}/test"
    response = client.post(test_url)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert response_json.get("connection_test")
    assert response_json.get("supported_version_test")
    assert response_json.get("success")


def test_update_mysql_source_connection(setup_and_teardown):
    create_url = "/source-connection/"
    data = {
        "type": "mysql",
        "table_name": "test_table",
        "user": "root",
        "password": "example",
        "host": "localhost",
        "port": 3306,
        "db": "",
    }
    response = client.post(create_url, json=data)
    response_json = response.json()

    update_url = f"/source-connection/{response_json.get('id')}"
    update_data = {"table_name": "test_table_update"}
    response = client.patch(update_url, json=update_data)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert response_json.get("table_name") == update_data["table_name"]


def test_update_postgresql_source_connection(setup_and_teardown):
    create_url = "/source-connection/"
    data = {
        "type": "postgresql",
        "schema_name": "test_schema",
        "table_name": "test_table",
        "user": "postgres",
        "password": "example",
        "host": "localhost",
        "port": 5432,
        "db": "",
    }
    response = client.post(create_url, json=data)
    response_json = response.json()

    update_url = f"/source-connection/{response_json.get('id')}"
    update_data = {"table_name": "test_table_update"}
    response = client.patch(update_url, json=update_data)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert response_json.get("table_name") == update_data["table_name"]


def test_read_mysql_source_connection_tables(setup_and_teardown):
    create_url = "/source-connection/"
    data = {
        "type": "mysql",
        "table_name": "test_table",
        "user": "root",
        "password": "example",
        "host": "localhost",
        "port": 3306,
        "db": "",
    }
    response = client.post(create_url, json=data)
    response_json = response.json()

    table_url = f"/source-connection/{response_json.get('id')}/tables"
    response = client.get(table_url)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert isinstance(response_json, list)


def test_read_postgresql_source_connection_tables(setup_and_teardown):
    create_url = "/source-connection/"
    data = {
        "type": "postgresql",
        "schema_name": "test_schema",
        "table_name": "test_table",
        "user": "postgres",
        "password": "example",
        "host": "localhost",
        "port": 5432,
        "db": "",
    }
    response = client.post(create_url, json=data)
    response_json = response.json()

    table_url = f"/source-connection/{response_json.get('id')}/tables"
    response = client.get(table_url)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert isinstance(response_json, list)


def test_read_mysql_source_connection_table_schema(setup_and_teardown):
    create_url = "/source-connection/"
    data = {
        "type": "mysql",
        "table_name": "test_table",
        "user": "root",
        "password": "example",
        "host": "localhost",
        "port": 3306,
        "db": "",
    }
    response = client.post(create_url, json=data)
    response_json = response.json()

    table_url = f"/source-connection/{response_json.get('id')}/table-schema"
    response = client.get(table_url)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert isinstance(response_json, list)


def test_read_postgresql_source_connection_table_schema(setup_and_teardown):
    create_url = "/source-connection/"
    data = {
        "type": "postgresql",
        "schema_name": "test_schema",
        "table_name": "test_table",
        "user": "postgres",
        "password": "example",
        "host": "localhost",
        "port": 5432,
        "db": "",
    }
    response = client.post(create_url, json=data)
    response_json = response.json()

    table_url = f"/source-connection/{response_json.get('id')}/table-schema"
    response = client.get(table_url)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert isinstance(response_json, list)


def test_read_mysql_source_connection_rows(setup_and_teardown):
    create_url = "/source-connection/"
    data = {
        "type": "mysql",
        "table_name": "test_table",
        "user": "root",
        "password": "example",
        "host": "localhost",
        "port": 3306,
        "db": "",
    }
    response = client.post(create_url, json=data)
    response_json = response.json()

    table_url = f"/source-connection/{response_json.get('id')}/rows"
    response = client.get(table_url)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert isinstance(response_json, list)


def test_read_postgresql_source_connection_rows(setup_and_teardown):
    create_url = "/source-connection/"
    data = {
        "type": "postgresql",
        "schema_name": "test_schema",
        "table_name": "test_table",
        "user": "postgres",
        "password": "example",
        "host": "localhost",
        "port": 5432,
        "db": "",
    }
    response = client.post(create_url, json=data)
    response_json = response.json()

    table_url = f"/source-connection/{response_json.get('id')}/rows"
    response = client.get(table_url)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert isinstance(response_json, list)


def test_delete_mysql_source_connection(setup_and_teardown):
    create_url = "/source-connection/"
    data = {
        "type": "mysql",
        "table_name": "test_table",
        "user": "root",
        "password": "example",
        "host": "localhost",
        "port": 3306,
        "db": "",
    }
    response = client.post(create_url, json=data)
    response_json = response.json()
    data_id = response_json.get("id")

    delete_url = f"/source-connection/{data_id}"
    response = client.delete(delete_url)
    assert response.status_code == 200, response.text

    response = client.delete(delete_url)
    assert response.status_code == 404, response.text


def test_delete_postgresql_source_connection(setup_and_teardown):
    create_url = "/source-connection/"
    data = {
        "type": "postgresql",
        "schema_name": "test_schema",
        "table_name": "test_table",
        "user": "postgres",
        "password": "example",
        "host": "localhost",
        "port": 5432,
        "db": "",
    }
    response = client.post(create_url, json=data)
    response_json = response.json()
    data_id = response_json.get("id")

    delete_url = f"/source-connection/{data_id}"
    response = client.delete(delete_url)
    assert response.status_code == 200, response.text

    response = client.delete(delete_url)
    assert response.status_code == 404, response.text
