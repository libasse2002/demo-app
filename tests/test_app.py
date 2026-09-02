import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app import app  


def test_homepage_returns_200():
    client = app.test_client()
    resp = client.get("/")
    assert resp.status_code == 200
    assert b"Demo App" in resp.data