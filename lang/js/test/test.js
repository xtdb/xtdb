import assert from 'assert';
import postgres from 'postgres';
import tjs from 'transit-js';

describe("connects to XT", function() {

  let sql;

  before (async () => {
    sql = postgres({
      host: "localhost",
      port: process.env.PG_PORT,
      fetch_types: false, // currently required https://github.com/xtdb/xtdb/issues/3607
      types: {
        bool: {to: 16},
        int: {
          to: 20,
          from: [23, 20], // int4, int8
          parse: parseInt
        },
        transit: {
          to: 16384,
          from: [16384],
          serialize: (v) => tjs.writer('json').write(v)
        }
      }
    })

    await sql`SELECT 1` // HACK https://github.com/porsager/postgres/issues/751
  })

  after(async () => {
    await sql.end()
  })

  it("should return the inserted row", async () => {
    const conn = await sql.reserve()

    try {
      await conn`INSERT INTO foo (_id, msg) VALUES (${conn.typed.int(1)}, 'Hello world!')`

      assert.deepStrictEqual([...await conn`SELECT _id, msg FROM foo`],
                             [{_id: 1, msg: 'Hello world!'}])

    } finally {
      await conn.release()
    }
  })

  it("JSON-like types can be roundtripped", async () => {
    const conn = await sql.reserve()
    try {
      await conn`INSERT INTO foo2 (_id, bool) VALUES (1, ${conn.typed.bool(true)})`

      assert.deepStrictEqual([{_id: 1, bool: true}],
                             [...await conn`SELECT * FROM foo2`])
    } finally {
      await conn.release()
    }

  })

  it("should round-trip JSON", async () => {
    const conn = await sql.reserve()
    try {
      await conn`INSERT INTO foo (_id, json) VALUES (${conn.typed.int(2)}, ${conn.json({a: 1})})`

      assert.deepStrictEqual([...await conn`SELECT _id, json FROM foo WHERE _id = 2`],
                             [{_id: 2, json: {a: 1}}])

      assert.deepStrictEqual([...await conn`SELECT _id, (json).a FROM foo WHERE _id = 2`],
                             [{_id: 2, a: 1}])
    } finally {
      await conn.release()
    }
  })

  it("should round-trip transit", async () => {
    const conn = await sql.reserve()

    try {
      const ts = new Date('2020-01-01')

      await conn`INSERT INTO foo (_id, transit) VALUES (${conn.typed.int(2)}, ${conn.typed.transit({ts})})`

      // no transit writer in the backend yet
      assert.deepStrictEqual([...await conn`SELECT _id, (transit).ts + INTERVAL 'P2D' AS third FROM foo WHERE _id = 2`],
                             [{_id: 2, third: "2020-01-03T00:00Z"}])

    } finally {
      await conn.release()
    }
  })
})
