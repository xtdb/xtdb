from xtdb.xt_json import XtdbJsonEncoder
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


def test_node_status(client):
    assert client.status()['latestCompletedTx'] is None


def test_sql_tx_and_query(client):
    query = Sql("SELECT * FROM people")
    assert client.query(query) == []

    client.submit_tx([Sql("INSERT INTO people (xt$id) VALUES ('Alice')")])

    assert client.query(query) == [{"xt$id": "Alice"}]

def test_null_coverage(client):
    query = Sql("SELECT * FROM people")
    assert client.query(query) == []

    client.submit_tx([Sql("INSERT INTO people (xt$id, name) VALUES ('Alice', null)")])

    assert client.query(query) == [{"xt$id": "Alice"}]


def test_independent_processing(make_client):
    client1 = make_client()
    client2 = make_client()

    query = Sql("SELECT * FROM people")
    assert client1.query(query) == []
    assert client2.query(query) == []

    client1.submit_tx([Sql("INSERT INTO people (xt$id) VALUES ('Alice')")])

    assert client1.query(query) == [{"xt$id": "Alice"}]
    assert client2.query(query) == []

def test_db_api_class(client):
    conn = DBAPI().connect(client._url)
    conn.execute("INSERT INTO docs (xt$id, foo) VALUES (1, 'bar')")
    result = conn.execute("SELECT * FROM docs")
    assert result.fetchall() == [['bar', 1]]

def test_timestamps(client):
    client.submit_tx([Sql("INSERT INTO trades (xt$id, price) VALUES (1, 100)")])
    query_result = client.query(Sql("SELECT trades.xt$id, trades.price, trades.xt$system_from, trades.xt$system_to FROM trades FOR ALL SYSTEM_TIME"))
    assert isinstance(query_result[0]["xt$system_from"], datetime)

def test_basis_change(client):
    client.set_tx_time("2020-01-01T12:34:56Z")
    client.submit_tx([Sql("INSERT INTO trades (xt$id, price) VALUES (1, 100)")])
    client.set_tx_time("2020-01-02T12:34:56Z")
    client.submit_tx([Sql("INSERT INTO trades (xt$id, price) VALUES (1, 105)")])
    query_result = client.query(Sql("SELECT trades.xt$id, trades.price, trades.xt$system_from, trades.xt$system_to FROM trades FOR ALL SYSTEM_TIME"))
    assert query_result[0]['xt$system_from'].day - query_result[1]['xt$system_from'].day == 1
    client.set_basis("2020-01-02T12:34:55Z")
    query_result = client.query(Sql("SELECT trades.xt$id, trades.price FROM trades"))
    assert query_result[0]['price'] == 100

def test_tx_time(client):
    tx = client.submit_tx([Sql("INSERT INTO trades (xt$id, price) VALUES (1, 100)")])
    tx2 = client.submit_tx([Sql("INSERT INTO trades (xt$id, price) VALUES (1, 105)")])
    client.set_at_tx(tx)
    assert client._at_tx["txId"] == tx.tx_id
    assert client._at_tx["systemTime"] == datetime.strftime(tx.system_time, "%Y-%m-%dT%H:%M:%SZ")

    query_result = client.query(Sql("SELECT trades.xt$id, trades.price FROM trades FOR ALL SYSTEM_TIME"))

    assert len(query_result) == 0


    
