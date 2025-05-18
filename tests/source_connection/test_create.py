from fastapi.testclient import TestClient

from app.dependencies import get_session
from app.main import app
from app.validators import Error as ValidationError
from tests.conftest import get_session_replacement
from tests.factories.source_connection_factory import SourceConnectionFactory

client = TestClient(app)

app.dependency_overrides[get_session] = get_session_replacement

factory = SourceConnectionFactory()
mysql_conn = factory.get_source_connection("mysql")
postgresql_conn = factory.get_source_connection("postgresql")
url = "/source-connection/"


def test_mysql_support():
    response = client.post(url, json=mysql_conn)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert "id" in response_json

    response = client.post(f"{url}{response_json.get('id')}/test")
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert response_json.get("success")
    assert not response_json.get("password")


def test_postgresql_support():
    response = client.post(url, json=postgresql_conn)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert "id" in response_json

    response = client.post(f"{url}{response_json.get('id')}/test")
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert response_json.get("success")
    assert not response_json.get("password")


def test_mysql_required_fields():
    response = client.post(
        url,
        json={key: mysql_conn[key] for key in mysql_conn if key not in ["table_name"]},
    )
    response_json = response.json()
    assert response.status_code == 422, response.text
    assert response_json.get("detail") == ValidationError.TABLE_REQUIRED_ERROR


def test_postgresql_required_fields():
    response = client.post(
        url,
        json={
            key: postgresql_conn[key]
            for key in postgresql_conn
            if key not in ["table_name"]
        },
    )
    response_json = response.json()
    assert response.status_code == 422, response.text
    assert response_json.get("detail") == ValidationError.TABLE_REQUIRED_ERROR

    response = client.post(
        url,
        json={
            key: postgresql_conn[key]
            for key in postgresql_conn
            if key not in ["schema_name"]
        },
    )
    response_json = response.json()
    assert response.status_code == 422, response.text
    assert response_json.get("detail") == ValidationError.SCHEMA_REQUIRED_ERROR


def test_not_supported_type():
    response = client.post(url, json={**mysql_conn, "type": "sqlite"})
    assert response.status_code == 422, response.text
