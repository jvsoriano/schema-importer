from fastapi.testclient import TestClient

from app.dependencies import get_session
from app.main import app
from tests.conftest import get_session_replacement
from tests.factories.source_connection_factory import SourceConnectionFactory

client = TestClient(app)

app.dependency_overrides[get_session] = get_session_replacement

factory = SourceConnectionFactory()
mysql_conn = factory.get_source_connection("mysql")
url = "/source-connection/{0}"


def test_retrieval_of_available_tables():
    response = client.post(url.format(""), json=mysql_conn)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert "id" in response_json

    response = client.get(url.format(response_json.get("id")) + "/tables")
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert isinstance(response_json, list)


def test_retrieval_of_table_schema():
    response = client.post(url.format(""), json=mysql_conn)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert "id" in response_json

    response = client.get(url.format(response_json.get("id")) + "/table-schema")
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert isinstance(response_json, list)

    if len(response_json):
        assert "name" in response_json[0]
        assert "type" in response_json[0]
        assert "primary_key" in response_json[0]


def test_retrieval_of_table_rows():
    response = client.post(url.format(""), json=mysql_conn)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert "id" in response_json

    response = client.get(url.format(response_json.get("id")) + "/rows")
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert isinstance(response_json, list)


def test_retrieval_of_table_rows_invalid_limit():
    response = client.post(url.format(""), json=mysql_conn)
    response_json = response.json()
    assert response.status_code == 200, response.text
    assert "id" in response_json

    response = client.get(url.format(response_json.get("id")) + "/rows?limit=200")
    assert response.status_code == 422, response.text
