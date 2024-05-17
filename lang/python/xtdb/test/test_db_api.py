from xtdb import DBAPI, SQLParsingError

import pytest

def test_db_api_class(client):
    conn = DBAPI().connect(client._url)
    conn.execute("INSERT INTO docs (xt$id, foo) VALUES (1, 'bar');")
    result = conn.execute("SELECT * FROM docs;")
    assert result.fetchall() == [['bar', 1]]


def test_multiple_statements_db_api(client):
    conn = DBAPI().connect(client._url)
    conn.execute("""INSERT INTO docs (xt$id, foo) VALUES (1, 'bar');
                    INSERT INTO docs (xt$id, foo) VALUES (1, 'baz');""")
    
    result = conn.execute("SELECT * FROM docs;")
    assert result.fetchall() == [['baz', 1]]


def test_import_system_time_session_var_api(client):
    conn = DBAPI().connect(client._url)    
    conn.execute("INSERT INTO docs (xt$id, foo) VALUES (1, 'bang');")
    conn.execute("""SET import_system_time = '3001-01-02T12:34:56Z';
                    INSERT INTO docs (xt$id, foo) VALUES (1, 'bar');
                    INSERT INTO docs (xt$id, foo) VALUES (1, 'baz');
                    """)
    result = conn.execute("SELECT * FROM docs;")
    assert result.fetchall() == [['bang', 1]]

def test_basis_session_var_api(client):
    conn = DBAPI().connect(client._url)
    conn.execute("""SET import_system_time = '2020-01-02T12:34:56Z';
                    INSERT INTO docs (xt$id, foo) VALUES (1, 'bar');
                    INSERT INTO docs (xt$id, foo) VALUES (1, 'b;az');
                    """)

    conn.execute("""SET import_system_time = '2021-01-03T12:34:56Z';
                    INSERT INTO docs (xt$id, foo) VALUES (1, 'bang');
                    """)

    result = conn.execute("""
                          SET basis = '2021-01-03T12:30:56Z';
                          SELECT * FROM docs;
                          """)

    assert result.fetchall() == [['b;az', 1]]


def test_reversion_of_session_vars_api(client):
    conn = DBAPI().connect(client._url)
    conn.client.set_basis("2021-01-02T12:30:56Z")
    assert conn.client._basis == "2021-01-02T12:30:56Z"
    result = conn.execute("""
                          SET basis = None;
                          """)

    assert conn.client._basis == None

def test_set_statements_at_top_only(client):
    conn = DBAPI().connect(client._url)
    try:
        ex = conn.execute("""
                          SET basis = None;
                          SELECT * FROM docs;
                          SET basis = None;
                          """)
        assert False
    except SQLParsingError:
        assert True
