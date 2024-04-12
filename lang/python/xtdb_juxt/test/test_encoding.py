from xtdb_juxt.xt_json import XtdbJsonEncoder
from xtdb_juxt.query import From
from xtdb_juxt import Xtdb
from xtdb_juxt.types import Sql

import os
import pytest
import requests

database_host = os.getenv('DATABASE_HOST', 'localhost')
database_port = os.getenv('DATABASE_PORT', '3300')

def setup_xt_proxy(*tokens):
    def inner_decorator(f):
        base_url = f"http://{database_host}:{database_port}"
        def wrapper():
            clients = {}
            
            for token in tokens:
                # Setup
                requests.get(base_url + f"/setup?{token}")
                client = Xtdb(base_url + "/" + token)
                clients[token] = client

            # Call
            f(clients)

            for token in tokens:
                # Teardown
                requests.get(base_url + f"/teardown?{token}")
                
        return wrapper
    return inner_decorator

client = Xtdb(f"http://{database_host}:{database_port}")

def test_encoding():
    query = From("people", None, None, True)
    assert query.to_json()['from'] == 'people'

@setup_xt_proxy("node_1")
def test_node_status(clients):
    assert clients["node_1"].status()['latestCompletedTx'] == None

@setup_xt_proxy("node_1")
def test_xtql_query_from(clients):
    query = From("people", None, None, True)
    assert clients["node_1"].query(query.to_json()) == []

# @setup_xt_proxy("node_1", "node_2")
# def test_independent_processing(clients)

