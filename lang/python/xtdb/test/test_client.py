from xtdb.types import Sql

from datetime import datetime
import pytest

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

def test_timestamps(client):
    client.submit_tx([Sql("INSERT INTO trades (xt$id, price) VALUES (1, 100)")])
    query_result = client.query(Sql("SELECT trades.xt$id, trades.price, trades.xt$system_from, trades.xt$system_to FROM trades FOR ALL SYSTEM_TIME"))
    assert isinstance(query_result[0]["xt$system_from"], datetime)

def test_basis_change(client):
    client.set_import_system_time("2020-01-01T12:34:56Z")
    client.submit_tx([Sql("INSERT INTO trades (xt$id, price) VALUES (1, 100)")])
    client.set_import_system_time("2020-01-02T12:34:56Z")
    client.submit_tx([Sql("INSERT INTO trades (xt$id, price) VALUES (1, 105)")])
    query_result = client.query(Sql("SELECT trades.xt$id, trades.price, trades.xt$system_from, trades.xt$system_to FROM trades FOR ALL SYSTEM_TIME"))
    assert query_result[0]['xt$system_from'].day - query_result[1]['xt$system_from'].day == 1
    client.set_basis("2020-01-02T12:34:55Z")
    query_result = client.query(Sql("SELECT trades.xt$id, trades.price FROM trades"))
    assert query_result[0]['price'] == 100

def test_at_tx(client):
    tx = client.submit_tx([Sql("INSERT INTO trades (xt$id, price) VALUES (1, 100)")])
    tx2 = client.submit_tx([Sql("INSERT INTO trades (xt$id, price) VALUES (1, 105)")])
    client.set_at_tx(tx)
    assert client._at_tx["txId"] == tx.tx_id
    assert client._at_tx["systemTime"] == datetime.strftime(tx.system_time, "%Y-%m-%dT%H:%M:%SZ")

    query_result = client.query(Sql("SELECT trades.xt$id, trades.price FROM trades FOR ALL SYSTEM_TIME"))

    assert len(query_result) == 0
