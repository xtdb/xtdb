from xtdb_juxt.xt_json import XtdbJsonEncoder
from xtdb_juxt.query import From
from xtdb_juxt import Xtdb
from xtdb_juxt.types import Sql

import os
import pytest

database_host = os.getenv('DATABASE_HOST', 'localhost')
database_port = os.getenv('DATABASE_PORT', '3000')

client = Xtdb(f"http://{database_host}:{database_port}")

def test_encoding():
    encoder = XtdbJsonEncoder()
    query = From("people", None, None, True)
    print(query.to_json())
    # assert query.to_json() == {}

def test_node_status():
    assert client.status()['latestCompletedTx']

def test_node_sql_transaction_and_query():
    transaction = Sql("INSERT INTO p (xt$id, name) VALUES (1, 'John')")
    query = Sql("SELECT * FROM p WHERE p.xt$id = 1")
    
    assert isinstance(client.submit_tx([transaction]).tx_id, int)
    assert client.query(query)[0] == {'name': 'John', 'xt/id': 1}
