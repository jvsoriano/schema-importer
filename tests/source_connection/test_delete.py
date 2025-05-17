from fastapi.testclient import TestClient

from app.dependencies import get_session
from app.main import app
from app.models.source_connection import SourceConnectionError
from tests.conftest import get_session_replacement
from tests.factories.source_connection_factory import SourceConnectionFactory

client = TestClient(app)

app.dependency_overrides[get_session] = get_session_replacement

factory = SourceConnectionFactory()
mysql_conn = factory.get_source_connection("mysql")
mysql_data = None
postgresql_conn = factory.get_source_connection("postgresql")
postgresql_data = None
url = "/source-connection/{0}"


def test_delete_existing():
    response = client.post(url.format(""), json=mysql_conn)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert "id" in response_json

    response_id = response_json.get("id")

    response = client.delete(url.format(response_json.get("id")))
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert response_json.get("success")

    response = client.post(url.format(response_id) + "/test")
    response_json = response.json()
    assert response.status_code == 404, response.text
    assert response_json.get("detail") == SourceConnectionError.NOT_FOUND


def test_delete_not_existing():
    response = client.delete(url.format("not_existing_id"))
    response_json = response.json()
    assert response.status_code == 404, response.text
    assert response_json.get("detail") == SourceConnectionError.NOT_FOUND
