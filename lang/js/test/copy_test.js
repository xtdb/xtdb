import assert from 'assert';
import postgres from 'postgres';
import tjs from 'transit-js';
import * as uuid from 'uuid';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';

const transitReadHandlers = {
  'time/zoned-date-time': (s) => new Date(s.replace(/\[.+\]$/, '')),
}

let sql;

beforeEach(async () => {
  sql = postgres({
    host: process.env.PG_HOST || "localhost",
    port: process.env.PG_PORT || 5439,
    database: uuid.v4().toString(),
    fetch_types: false,
    types: {
      bool: {to: 16},
      int: {
        to: 20,
        from: [23, 20],
        parse: parseInt
      },
      transit: {
        to: 16384,
        from: [16384],
        serialize: (v) => tjs.writer('json').write(v),
        parse: (v) => tjs.reader('json', { handlers: transitReadHandlers }).read(v)
      }
    }
  })

  await sql`SELECT 1`
})

afterEach(async () => {
  await sql.end()
})

describe("COPY with transit-json format", function() {
  it("should copy records with only _id using transit-json format #4677", async () => {
    const conn = await sql.reserve()

    try {
      const testData = [
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

      const writer = tjs.writer('json');
      const lines = testData.map(record => writer.write(record));
      const transitData = lines.join('\n');

      const transitStream = Readable.from([transitData])
      const query = await conn`COPY test_items_only_id FROM STDIN WITH (FORMAT 'transit-json')`.writable()
      await pipeline(transitStream, query)

      // See https://github.com/porsager/postgres/pull/1016
      await new Promise(resolve => setTimeout(resolve, 50))

      const result = await conn`SELECT * FROM test_items_only_id ORDER BY _id`;
      
      assert.deepStrictEqual([...result], [
        {"_id": "item-3", "title": "Test Item 3", "price": 199.99},
        {"_id": "item-4", "title": "Test Item 4", "price": 249.99}
      ]);
      
    } finally {
      await conn.release()
    }
  })
})
