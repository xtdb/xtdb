import psycopg as pg
import json
import os
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
