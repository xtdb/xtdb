from xtdb import DBAPI, SQLParsingError

import pytest

def test_db_api_class(connection):
    connection.execute("INSERT INTO docs (xt$id, foo) VALUES (1, 'bar');")
    result = connection.execute("SELECT * FROM docs;")
    assert result.fetchall() == [[1, 'bar']]


def test_multiple_statements_db_api(connection):
    connection.execute("""INSERT INTO docs (xt$id, foo) VALUES (1, 'bar');
                    INSERT INTO docs (xt$id, foo) VALUES (1, 'baz');""")
    
    result = connection.execute("SELECT * FROM docs;")
    assert result.fetchall() == [[1, 'baz']]


def test_import_system_time_session_var_api(connection):
    connection.execute("INSERT INTO docs (xt$id, foo) VALUES (1, 'bang');")
    
    connection.execute("""SET import_system_time = '3001-01-02T12:34:56Z';
                    INSERT INTO docs (xt$id, foo) VALUES (1, 'bar');
                    INSERT INTO docs (xt$id, foo) VALUES (1, 'baz');
                    """)
    result = connection.execute("SELECT * FROM docs;")
    assert result.fetchall() == [[1, 'bang']]

def test_basis_session_var_api(connection):
    connection.execute("""SET import_system_time = '2020-01-02T12:34:56Z';
                    INSERT INTO docs (xt$id, foo) VALUES (1, 'bar');
                    INSERT INTO docs (xt$id, foo) VALUES (1, 'b;az');
                    """)

    connection.execute("""SET import_system_time = '2021-01-03T12:34:56Z';
                    INSERT INTO docs (xt$id, foo) VALUES (1, 'bang');
                    """)

    result = connection.execute("""
                          SET basis = '2021-01-03T12:30:56Z';
                          SELECT * FROM docs;
                          """)

    assert result.fetchall() == [[1, 'b;az']]


def test_reversion_of_session_vars_api(connection):
    connection.client.set_basis("2021-01-02T12:30:56Z")
    assert connection.client._basis == "2021-01-02T12:30:56Z"
    result = connection.execute("""
                          SET basis = None;
                          """)

    assert connection.client._basis == None

def test_set_statements_at_top_only(connection):
    try:
        ex = connection.execute("""
                          SET basis = None;
                          SELECT * FROM docs;
                          SET basis = None;
                          """)
        assert False
    except SQLParsingError:
        assert True

def test_as_of_query(connection):
    connection.execute("SET import_system_time = '2020-01-01T00:00:00Z'; INSERT INTO trades (xt$id, price) VALUES (1, 100);")
    connection.execute("SET import_system_time = '2020-01-02T00:00:00Z'; INSERT INTO trades (xt$id, price) VALUES (1, 150);")

    result = connection.execute("SELECT trades.xt$id, trades.price FROM trades FOR SYSTEM_TIME AS OF DATE '2020-01-01';")

    assert result.fetchall() == [[1, 100]]
