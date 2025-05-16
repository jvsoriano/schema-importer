import copy

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, StaticPool, create_engine

# from app.dependencies import get_session
from app.dependencies import get_session
from app.main import app
from app.models.source_connection import SourceConnectionError

client = TestClient(app)

# sqlite_file_name = "app.test.db"
# sqlite_url = f"sqlite:///{sqlite_file_name}"
sqlite_url = "sqlite:///:memory:"

connect_args = {"check_same_thread": False}
engine = create_engine(sqlite_url, connect_args=connect_args, poolclass=StaticPool)


# Create database tables
# def create_db_and_tables():
#     SQLModel.metadata.create_all(engine)


def get_session_replacement():
    with Session(engine) as session:
        yield session


# SessionDepReplacement = Annotated[Session, Depends(get_session)]
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


def test_test_new_source_connection(setup_and_teardown):
    url = "/source-connection/test"
    mysql_data = {
        "type": "mysql",
        "table_name": "test_table",
        "user": "root",
        "password": "example",
        "host": "localhost",
        "port": 3306,
        "db": "",
    }
    response = client.post(url, json=mysql_data)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert response_json.get("connectivity_test")
    assert response_json.get("supported_version_test")
    assert response_json.get("success")

    postgres_data = copy.deepcopy(mysql_data)
    postgres_data["type"] = "postgresql"
    postgres_data["schema_name"] = "test_schema"
    postgres_data["user"] = "postgres"
    postgres_data["port"] = 5432
    response = client.post(url, json=postgres_data)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert response_json.get("connectivity_test")
    assert response_json.get("supported_version_test")
    assert response_json.get("success")


def test_test_existing_source_connection(setup_and_teardown):
    create_url = "/source-connection/"
    mysql_data = {
        "type": "mysql",
        "table_name": "test_table",
        "user": "root",
        "password": "example",
        "host": "localhost",
        "port": 3306,
        "db": "",
    }
    response = client.post(create_url, json=mysql_data)
    response_json = response.json()

    test_url = f"/source-connection/{response_json.get('id')}/test"
    response = client.post(test_url)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert response_json.get("connectivity_test")
    assert response_json.get("supported_version_test")
    assert response_json.get("success")

    postgres_data = copy.deepcopy(mysql_data)
    postgres_data["type"] = "postgresql"
    postgres_data["schema_name"] = "test_schema"
    postgres_data["user"] = "postgres"
    postgres_data["port"] = 5432

    response = client.post(create_url, json=mysql_data)
    response_json = response.json()

    test_url = f"/source-connection/{response_json.get('id')}/test"
    response = client.post(test_url)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert response_json.get("connectivity_test")
    assert response_json.get("supported_version_test")
    assert response_json.get("success")


def test_update_source_connection(setup_and_teardown):
    create_url = "/source-connection/"
    mysql_data = {
        "type": "mysql",
        "table_name": "test_table",
        "user": "root",
        "password": "example",
        "host": "localhost",
        "port": 3306,
        "db": "",
    }
    response = client.post(create_url, json=mysql_data)
    response_json = response.json()

    update_url = f"/source-connection/{response_json.get('id')}"
    update_data = {"table_name": "test_table_update"}
    response = client.patch(update_url, json=update_data)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert response_json.get("table_name") == update_data["table_name"]

    postgres_data = copy.deepcopy(mysql_data)
    postgres_data["type"] = "postgresql"
    postgres_data["schema_name"] = "test_schema"
    postgres_data["user"] = "postgres"
    postgres_data["port"] = 5432

    response = client.post(create_url, json=postgres_data)
    response_json = response.json()

    update_url = f"/source-connection/{response_json.get('id')}"
    update_data = {"schema_name": "test_schema_update"}
    response = client.patch(update_url, json=update_data)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert response_json.get("schema_name") == update_data["schema_name"]


# def test_create_source_connection_type_mysql_valid(setup_and_teardown):
#     url = "/source-connection/"
#     data = {
#         "type": "mysql",
#         "table_name": "test_table",
#         "user": "root",
#         "password": "example",
#         "host": "localhost",
#         "port": 3306,
#         "db": "",
#     }
#     response = client.post(url, json=data)
#     assert response.status_code == 200, response.text
#     assert "id" in response.json()


# def test_create_source_connection_type_mysql_invalid(setup_and_teardown):
#     url = "/source-connection/"
#     data = {
#         "type": "mysql",
#         "table_name": "",
#         "user": "root",
#         "password": "example",
#         "host": "localhost",
#         "port": 3306,
#         "db": "",
#     }
#     response = client.post(url, json=data)
#     assert response.status_code == 422, response.text


# def test_create_source_connection_type_postgresql_valid(setup_and_teardown):
#     url = "/source-connection/"
#     data = {
#         "type": "postgresql",
#         "schema_name": "test_schema",
#         "table_name": "test_table",
#         "user": "postgres",
#         "password": "example",
#         "host": "localhost",
#         "port": 5432,
#         "db": "",
#     }
#     response = client.post(url, json=data)
#     assert response.status_code == 200, response.text
#     assert "id" in response.json()


# def test_create_source_connection_type_postgresql_invalid(setup_and_teardown):
#     url = "/source-connection/"
#     data_no_schema = {
#         "type": "postgresql",
#         "schema_name": "",
#         "table_name": "test_table",
#         "user": "postgres",
#         "password": "example",
#         "host": "localhost",
#         "port": 5432,
#         "db": "",
#     }
#     response = client.post(url, json=data_no_schema)
#     assert response.status_code == 422, response.text

#     data_no_table = {
#         "type": "postgresql",
#         "schema_name": "test_schema",
#         "table_name": "",
#         "user": "postgres",
#         "password": "example",
#         "host": "localhost",
#         "port": 5432,
#         "db": "",
#     }
#     response = client.post(url, json=data_no_table)
#     assert response.status_code == 422, response.text

#     data_no_schema_and_table = {
#         "type": "postgresql",
#         "schema_name": "",
#         "table_name": "",
#         "user": "postgres",
#         "password": "example",
#         "host": "localhost",
#         "port": 5432,
#         "db": "",
#     }
#     response = client.post(url, json=data_no_schema_and_table)
#     assert response.status_code == 422, response.text


# def test_create_source_connection_type_invalid(setup_and_teardown):
#     url = "/source-connection/"
#     data = {
#         "type": "invalid",
#         "schema_name": "test_schema",
#         "table_name": "test_table",
#         "user": "postgres",
#         "password": "example",
#         "host": "localhost",
#         "port": 5432,
#         "db": "",
#     }
#     response = client.post(url, json=data)
#     assert response.status_code == 422, response.text
