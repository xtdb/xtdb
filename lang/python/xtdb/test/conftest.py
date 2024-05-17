from xtdb import Xtdb

import pytest
import os
import requests

database_host = os.getenv('DATABASE_HOST', 'localhost')
database_port = os.getenv('DATABASE_PORT', '3300')
base_url = f"http://{database_host}:{database_port}"


@pytest.fixture
def make_client():
    tokens = []

    # Setup
    def _make_client():
        resp = requests.get(base_url + "/setup")
        token = resp.text
        tokens.append(token)
        return Xtdb(base_url + "/" + token)

    yield _make_client

    # Tear down
    for token in tokens:
        requests.get(base_url + f"/teardown?{token}")


@pytest.fixture
def client(make_client):
    return make_client()
