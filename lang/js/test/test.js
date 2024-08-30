import assert from 'assert';
import postgres from 'postgres';

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
        json: {
          to: 114,
          from: [114],
          serialize: JSON.stringify,
          parse: JSON.parse
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

    await conn`INSERT INTO foo (_id, msg) VALUES (${sql.typed.int(1)}, 'Hello world!')`

    assert.deepStrictEqual([...await conn`SELECT _id, msg FROM foo`],
                           [{_id: 1, msg: 'Hello world!'}])

    await conn.release()
  })

  /*it("JSON-like types can be roundtripped", async () => {
    await sql`INSERT INTO foo2 (_id, bool) VALUES (1, ${sql.typed.bool(true)})`

    assert.deepStrictEqual([...await sql`SELECT * FROM foo2`],
                           [{_id: '1', bool: true}])
  })*/

  it("should round-trip JSON", async () => {
    const conn = await sql.reserve()
    await conn`INSERT INTO foo (_id, json) VALUES (${sql.typed.int(2)}, ${sql.typed.json({a: 1})})`

    assert.deepStrictEqual([...await conn`SELECT _id, json FROM foo WHERE _id = 2`],
                           [{_id: 2, json: {a: 1}}])

    assert.deepStrictEqual([...await conn`SELECT _id, (json).a FROM foo WHERE _id = 2`],
                           [{_id: 2, a: 1}])
    await conn.release()
  })
})
