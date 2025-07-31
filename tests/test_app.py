import sys
import os
import pytest

# Ensure the parent directory is in sys.path so flask_app can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from py_de_id.app import create_app


@pytest.fixture
def client():
    app = create_app()
    app.config["TESTING"] = True
    return app.test_client()


def test_hello_world(client):
    rv = client.get("/hello-world")
    assert rv.status_code == 200
    assert rv.data == b"hello world."


def test_health(client):
    rv = client.get("/health")
    assert rv.status_code == 200


def test_deidentify_stub(client):
    rv = client.post(
        "/deidentify/123-xyz",
        json={
            "transaction_id": "test",
            "source_url": "http://consumer:8103/fhir/R4",
            "source_token": "example-token",
            "target_url": "http://customer:8103/fhir/R4",
            "target_token": "example-token",
            "patient_id": "123-xyz",
        },
    )
    assert rv.status_code == 200
    assert b"OK" in rv.data


def test_static_asset_happy_path(client):
    response = client.get("/favicon.ico")
    assert response.status_code == 200


def test_static_asset_sad_path(client):
    response = client.get("/favicon2.ico")
    assert response.status_code == 404
