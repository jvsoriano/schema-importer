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


def test_cannot_update_type():
    response = client.post(url.format(""), json=mysql_conn)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert "id" in response_json

    response = client.patch(
        url.format(response_json.get("id")), json={"type": "postgresql"}
    )
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert not response_json.get("type") == "postgresql"


def test_connection_before_update():
    response = client.post(url.format(""), json=mysql_conn)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert "id" in response_json

    response = client.patch(
        url.format(response_json.get("id")), json={"user": "invalid_user"}
    )
    response_json = response.json()
    assert response.status_code == 422, response.text
    assert response_json.get("detail") == SourceConnectionError.CONNECTION_FAILED
