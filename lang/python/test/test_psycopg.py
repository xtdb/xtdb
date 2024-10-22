import psycopg as pg
import os
import uuid

## Useful links
## https://www.psycopg.org/psycopg3/docs/basic/params.html
## https://www.psycopg.org/psycopg3/docs/api/types.html
## https://www.psycopg.org/psycopg3/docs/advanced/adapt.html

conn_params = {
    'dbname': str(uuid.uuid4()),
    'host': os.getenv('PG_HOST') or 'localhost',
    'port': os.getenv('PG_PORT') or 5439

}

def test_basic_query():
    with pg.connect(**conn_params) as conn:

        conn.autocommit = True

        with conn.cursor() as cur:

            cur.execute('SELECT 1')
            result = cur.fetchone()
            assert result == (1,)

def test_basic_query2():
    with pg.connect(**conn_params, prepare_threshold=0) as conn:

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
    with pg.connect(**conn_params, prepare_threshold=0) as conn:

        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute('SELECT %t', [1])
            assert cur.fetchall() == [(1,)]

        with conn.cursor() as cur:
            cur.execute('SELECT %b', [1])
            assert cur.fetchall() == [(1,)]

def test_string_parameters_3589():
    with pg.connect(**conn_params, prepare_threshold=0) as conn:

        conn.autocommit = True

        with conn.cursor() as cur:

            for x in range(4):
                cur.execute('''
                    INSERT INTO test_pg (_id, name)
                        VALUES (%s::INTEGER, %s::VARCHAR);
            ''',
                            (x, str(x)))

            cur.execute('SELECT * FROM test_pg;')
            assert set(cur.fetchall()) == {(0, '0'), (1, '1'), (2, '2'), (3, '3')}

            # TODO: this somehow fails because of schema change + prepared statement invalidation

            # for x in range(4):
            #     cur.execute('''
            #         INSERT INTO test_pg (_id, name)
            #             VALUES (%s::INTEGER, %s);
            # ''',
            #                 (x, x % 2 == 0))
            #
            # cur.execute('SELECT * FROM test_pg;')
            # assert set(cur.fetchall()) == {(0, True), (1, False), (2, True), (3, False)}

