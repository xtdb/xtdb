import psycopg as pg
import json
import os
import pyarrow as pa
import pytest
import uuid
import io
import transit.writer as writer

def conn_params():
    return {
        'dbname': str(uuid.uuid4()),
        'host': os.getenv('PG_HOST') or 'localhost',
        'port': os.getenv('PG_PORT') or 5439
    }


def test_copy_with_only_id_4677():
    test_data = [
        {
            "_id": "item-3",
            "title": "Test Item 3",
            "price": 199.99
        },
        {
            "_id": "item-4",
            "title": "Test Item 4",
            "price": 249.99
        }
    ]

    # NOTE: We need separate Writer instances per record due to a bug in transit-python2
    # where multiple writes to the same Writer add JSON array commas between records.
    # In transit-clj/transit-java you can write multiple records to one writer without
    # commas, but transit-python2 incorrectly treats them as array elements.
    lines = []
    for record in test_data:
        record_stream = io.StringIO()
        w = writer.Writer(record_stream, "json")
        w.write(record)
        lines.append(record_stream.getvalue())

    data_stream = io.StringIO('\n'.join(lines))
    data_stream.seek(0)

    with pg.connect(**conn_params()) as conn:
        conn.autocommit = True

        with conn.cursor(row_factory=pg.rows.dict_row) as cur:
            with cur.copy("COPY test_items_only_id FROM STDIN WITH (FORMAT 'transit-json')") as copy:
                copy.write(data_stream.getvalue())

            cur.execute('SELECT * FROM test_items_only_id ORDER BY _id')
            result = cur.fetchall()
            assert result == [{"_id": "item-3", "title": "Test Item 3", "price": 199.99},
                              {"_id": "item-4", "title": "Test Item 4", "price": 249.99}]


def test_copy_arrow_stream():
    """Test copying Arrow data using COPY FROM STDIN with arrow-stream format"""
    with pg.connect(**conn_params()) as conn:
        conn.autocommit = True

        # Create an Arrow table with some test data
        schema = pa.schema([
            ('_id', pa.int64()),
            ('name', pa.string()),
            ('value', pa.int64())
        ])

        data = pa.table({
            '_id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100, 200, 300]
        }, schema=schema)

        # Write the Arrow table to bytes using Arrow stream format
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_table(data)
        arrow_bytes = sink.getvalue().to_pybytes()

        # Use COPY to ingest the Arrow data
        with conn.cursor() as cur:
            with cur.copy("COPY test_arrow FROM STDIN WITH (FORMAT 'arrow-stream')") as copy:
                copy.write(arrow_bytes)

        # Query back the data to verify it was ingested correctly
        with conn.cursor() as cur:
            cur.execute('SELECT _id, name, value FROM test_arrow ORDER BY _id')
            result = cur.fetchall()
            assert result == [(1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)]


def test_copy_arrow_file():
    """Test copying Arrow data using COPY FROM STDIN with arrow-file format"""
    with pg.connect(**conn_params()) as conn:
        conn.autocommit = True

        # Create an Arrow table with some test data
        schema = pa.schema([
            ('_id', pa.int64()),
            ('name', pa.string()),
            ('value', pa.int64())
        ])

        data = pa.table({
            '_id': [10, 20, 30],
            'name': ['Dave', 'Eve', 'Frank'],
            'value': [400, 500, 600]
        }, schema=schema)

        # Write the Arrow table to bytes using Arrow file format
        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, schema) as writer:
            writer.write_table(data)
        arrow_bytes = sink.getvalue().to_pybytes()

        # Use COPY to ingest the Arrow data
        with conn.cursor() as cur:
            with cur.copy("COPY test_arrow_file FROM STDIN WITH (FORMAT 'arrow-file')") as copy:
                copy.write(arrow_bytes)

        # Query back the data to verify it was ingested correctly
        with conn.cursor() as cur:
            cur.execute('SELECT _id, name, value FROM test_arrow_file ORDER BY _id')
            result = cur.fetchall()
            assert result == [(10, 'Dave', 400), (20, 'Eve', 500), (30, 'Frank', 600)]


def test_copy_arrow_multiple_batches():
    """Test copying Arrow data with multiple record batches"""
    with pg.connect(**conn_params()) as conn:
        conn.autocommit = True

        # Create an Arrow schema
        schema = pa.schema([
            ('_id', pa.int64()),
            ('batch_num', pa.int64())
        ])

        # Write multiple batches to a stream
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as writer:
            # First batch
            batch1 = pa.record_batch([[1, 2], [1, 1]], schema=schema)
            writer.write_batch(batch1)

            # Second batch
            batch2 = pa.record_batch([[3, 4], [2, 2]], schema=schema)
            writer.write_batch(batch2)

        arrow_bytes = sink.getvalue().to_pybytes()

        # Use COPY to ingest the Arrow data
        with conn.cursor() as cur:
            with cur.copy("COPY test_multi_batch FROM STDIN WITH (FORMAT 'arrow-stream')") as copy:
                copy.write(arrow_bytes)

        # Query back the data to verify all batches were ingested
        with conn.cursor() as cur:
            cur.execute('SELECT _id, batch_num FROM test_multi_batch ORDER BY _id')
            result = cur.fetchall()
            assert result == [(1, 1), (2, 1), (3, 2), (4, 2)]
