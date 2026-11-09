import os
import uuid

import adbc_driver_flightsql.dbapi as flightsql
import pyarrow as pa


def connect(db_name, **db_kwargs):
    host = os.getenv('FLIGHT_SQL_HOST') or 'localhost'
    port = os.getenv('FLIGHT_SQL_PORT') or 3000
    return flightsql.connect(
        f'grpc://{host}:{port}',
        db_kwargs={'adbc.flight.sql.rpc.call_header.x-xtdb-database': db_name, **db_kwargs},
    )


def test_ingest_creates_a_table_on_a_fresh_database():
    with connect(str(uuid.uuid4())) as conn, conn.cursor() as cur:
        cur.adbc_ingest('foo', pa.table({'_id': [1, 2], 'v': ['a', 'b']}), mode='create_append')
        conn.commit()

        cur.execute('SELECT _id, v FROM foo ORDER BY _id')
        assert cur.fetch_arrow_table().to_pylist() == [{'_id': 1, 'v': 'a'}, {'_id': 2, 'v': 'b'}]


def test_executemany_of_a_parameterised_insert_lands():
    with connect(str(uuid.uuid4())) as conn, conn.cursor() as cur:
        cur.executemany('INSERT INTO foo (_id, v) VALUES (?, ?)', [(1, 'a'), (2, 'b')])
        conn.commit()

        cur.execute('SELECT _id, v FROM foo ORDER BY _id')
        assert cur.fetch_arrow_table().to_pylist() == [{'_id': 1, 'v': 'a'}, {'_id': 2, 'v': 'b'}]


def test_execute_of_a_literal_insert_lands():
    with connect(str(uuid.uuid4())) as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO foo (_id, v) VALUES (1, 'a')")
        conn.commit()

        cur.execute('SELECT _id, v FROM foo')
        assert cur.fetch_arrow_table().to_pylist() == [{'_id': 1, 'v': 'a'}]


def test_execute_of_a_parameterised_insert_lands():
    with connect(str(uuid.uuid4())) as conn, conn.cursor() as cur:
        cur.execute('INSERT INTO foo (_id, v) VALUES (?, ?)', (1, 'a'))
        conn.commit()

        cur.execute('SELECT _id, v FROM foo')
        assert cur.fetch_arrow_table().to_pylist() == [{'_id': 1, 'v': 'a'}]


def test_execute_of_set_time_zone_takes_effect():
    with connect(str(uuid.uuid4()), **{'adbc.flight.sql.rpc.with_cookie_middleware': 'true'}) as conn, \
            conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'Europe/London'")
        cur.execute('SHOW TIME ZONE')
        assert cur.fetchall() == [('Europe/London',)]

        cur.execute('SET TIME ZONE ?', ('America/New_York',))
        cur.execute('SHOW TIME ZONE')
        assert cur.fetchall() == [('America/New_York',)]
