from xtdb.xt_json import XtdbJsonEncoder
from xtdb.query import From
from xtdb.tx import PutDocs, Sql as SqlTx
from xtdb import Xtdb, DBAPI
from xtdb.types import Sql

from datetime import datetime
import os
import pytest
import requests
import uuid

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


def test_encoding():
    query = From("people", None, None, True)
    assert query.to_json()['from'] == 'people'


def test_node_status(client):
    assert client.status()['latestCompletedTx'] is None


def test_xtql_query_from(client):
    query = From("people", None, None, True)
    assert client.query(query) == []


def test_xtql_tx_and_query(client):
    query = From("people", None, None, True)
    assert client.query(query) == []

    client.submit_tx([PutDocs("people", {"xt$id": "Alice"})])

    assert client.query(query) == [{"xt$id": "Alice"}]


def test_sql_tx_and_query(client):
    query = Sql("SELECT * FROM people")
    assert client.query(query) == []

    client.submit_tx([SqlTx("INSERT INTO people (xt$id) VALUES ('Alice')")])

    assert client.query(query) == [{"xt$id": "Alice"}]

def test_null_coverage(client):
    query = Sql("SELECT * FROM people")
    assert client.query(query) == []

    client.submit_tx([SqlTx("INSERT INTO people (xt$id, name) VALUES ('Alice', null)")])

    assert client.query(query) == [{"xt$id": "Alice"}]


def test_independent_processing(make_client):
    client1 = make_client()
    client2 = make_client()

    query = Sql("SELECT * FROM people")
    assert client1.query(query) == []
    assert client2.query(query) == []

    client1.submit_tx([SqlTx("INSERT INTO people (xt$id) VALUES ('Alice')")])

    assert client1.query(query) == [{"xt$id": "Alice"}]
    assert client2.query(query) == []

def test_uuid_encoding_3327(client):
    sample_uuid = uuid.uuid4()
    client.submit_tx([PutDocs("uuid_test", {"xt$id": sample_uuid})])
    query = From("uuid_test", None, None, True)
    assert client.query(query) == [{"xt$id": sample_uuid}]

def test_db_api_class(client):
    conn = DBAPI().connect(client._url)
    conn.execute("INSERT INTO docs (xt$id, foo) VALUES (1, 'bar')")
    result = conn.execute("SELECT * FROM docs")
    assert result.fetchall() == [['bar', 1]]

def test_timestamps(client):
    client.submit_tx([SqlTx("INSERT INTO trades (xt$id, price) VALUES (1, 100)")])
    query_result = client.query(Sql("SELECT trades.xt$id, trades.price, trades.xt$system_from, trades.xt$system_to FROM trades FOR ALL SYSTEM_TIME"))
    assert isinstance(query_result[0]["xt$system_from"], datetime)
