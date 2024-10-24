import psycopg as pg
import os
import pytest
import uuid

## Useful links
## https://www.psycopg.org/psycopg3/docs/basic/params.html
## https://www.psycopg.org/psycopg3/docs/api/types.html
## https://www.psycopg.org/psycopg3/docs/advanced/adapt.html

def conn_params():
    return {
        'dbname': str(uuid.uuid4()),
        'host': os.getenv('PG_HOST') or 'localhost',
        'port': os.getenv('PG_PORT') or 5439
    }

def test_basic_query():
    with pg.connect(**conn_params()) as conn:

        conn.autocommit = True

        with conn.cursor() as cur:

            cur.execute('SELECT 1')
            result = cur.fetchone()
            assert result == (1,)

def test_basic_query2():
    with pg.connect(**conn_params(), prepare_threshold=0) as conn:

        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute('INSERT INTO foo(_id) VALUES (%s)', [1])

        with conn.cursor() as cur:
            cur.execute('INSERT INTO foo(_id) VALUES (1)')

        with conn.cursor() as cur:

            cur.execute('SELECT _id FROM foo')
            result = cur.fetchall()
            assert result ==  [(1,)]

        with conn.cursor() as cur:

            cur.execute('SELECT _id FROM foo FOR ALL VALID_TIME FOR ALL SYSTEM_TIME')
            result = cur.fetchall()
            assert result ==  [(1,), (1,), (1,)]

def test_integer_type():
    with pg.connect(**conn_params(), prepare_threshold=0) as conn:

        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute('SELECT %t', [1])
            assert cur.fetchall() == [(1,)]

        with conn.cursor() as cur:
            cur.execute('SELECT %b', [1])
            assert cur.fetchall() == [(1,)]

def test_execute_many_3597():
    with pg.connect(**conn_params(), prepare_threshold=0) as conn:

        data_to_insert = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')]
        insert_query = "INSERT INTO docs (_id, name) VALUES (%s, %s)"

        conn.adapters.register_dumper(str, pg.types.string.StrDumperVarchar)
        with conn.cursor() as cur:
            cur.executemany(insert_query, data_to_insert)
        # without autoCommit this gets transacted as one single transaction
        conn.commit()

        with conn.cursor() as cur:
            cur.execute('SELECT * FROM docs ORDER BY _id')
            assert cur.fetchall() == [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')]

            cur.execute('SELECT * FROM xt.txs ORDER BY _id')
            assert len(cur.fetchall()) == 1