from fastapi.testclient import TestClient

from app.dependencies import get_session
from app.main import app
from app.routers.source_connections import NOT_FOUND_ERROR
from tests.conftest import get_session_replacement
from tests.factories.source_connection_factory import SourceConnectionFactory

client = TestClient(app)

app.dependency_overrides[get_session] = get_session_replacement

factory = SourceConnectionFactory()
mysql_conn = factory.get_source_connection("mysql")
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
    assert response_json.get("detail") == NOT_FOUND_ERROR


def test_delete_not_existing():
    not_existing_id = 100
    response = client.delete(url.format(not_existing_id))
    response_json = response.json()
    assert response.status_code == 404, response.text
    assert response_json.get("detail") == NOT_FOUND_ERROR
