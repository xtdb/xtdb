import assert from 'assert';
import postgres from 'postgres';
import tjs from 'transit-js';

let sql;

beforeEach(async () => {
  sql = postgres({
    host: process.env.PG_HOST || "localhost",
    port: process.env.PG_PORT || 5439,
    fetch_types: false, // currently required https://github.com/xtdb/xtdb/issues/3607
    types: {
      bool: {to: 16},
      int: {
        to: 20,
        from: [23, 20], // int4, int8
        parse: parseInt
      },
      isoTimestamp: {
        // #3685
        to: 1184,
        serialize: (x) => x.toISOString(),
      },
    }
  })

  await sql`SELECT 1` // HACK https://github.com/porsager/postgres/issues/751
})

afterEach(async () => {
  await sql.end()
})


describe("accepts ISO-formatted timestamps, #3685", function() {

  it("accepts ISO-formatted timestamps", async () => {
    const conn = await sql.reserve()

    try {
      const ts = new Date('2020-01-01')
      await conn`INSERT INTO iso_timestamps_3685 RECORDS {_id: 1, iso: ${conn.types.isoTimestamp(ts)}}`

      assert.deepStrictEqual([...await conn`SELECT * FROM iso_timestamps_3685`],
                             [{_id: 1, iso: ts}])
    } finally {
      await conn.release()
    }
  })
})
