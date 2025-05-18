from unittest.mock import patch

from fastapi.testclient import TestClient

from app.dependencies import get_session
from app.main import app
from app.testers import SourceConnectionTester
from tests.conftest import get_session_replacement
from tests.factories.source_connection_factory import SourceConnectionFactory

client = TestClient(app)

app.dependency_overrides[get_session] = get_session_replacement

factory = SourceConnectionFactory()
mysql_conn = factory.get_source_connection("mysql")
url1 = "/source-connection/test"
url2 = "/source-connection/{0}/test"


def test_database_connection():
    response = client.post("/source-connection/", json=mysql_conn)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert "id" in response_json

    response = client.post(url2.format(response_json.get("id")), json=mysql_conn)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert "valid_credentials" in response_json
    assert "supported_version" in response_json
    assert "success" in response_json
    assert all(response_json.values())

    response = client.post(url1, json=mysql_conn)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert "valid_credentials" in response_json
    assert "supported_version" in response_json
    assert "success" in response_json
    assert all(response_json.values())


def test_invalid_credentials():
    response = client.post(
        url1,
        json={**mysql_conn, "user": "invalid_user", "password": "invalid_password"},
    )
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert not response_json.get("connection_test")
    assert not all(response_json.values())


def test_invalid_database():
    response = client.post(
        url1,
        json={**mysql_conn, "host": "invalid_host"},
    )
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert not response_json.get("connection_test")
    assert not all(response_json.values())


def mocked_version(*args):
    return 1.1


@patch.object(
    SourceConnectionTester, "_SourceConnectionTester__version", mocked_version
)
def test_non_supported_version():
    response = client.post(
        url1,
        json=mysql_conn,
    )
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert not response_json.get("supported_version_test")
    assert not all(response_json.values())
