import assert from 'assert';
import postgres from 'postgres';

describe("connects to XT", function() {

  let sql;

  before (() => {
    sql = postgres({
      host: "localhost",
      port: process.env.PG_PORT,
      fetch_types: false, // currently required https://github.com/xtdb/xtdb/issues/3607
      types: {
        int: { to: 23 }
      }
    })
  })

  after(async () => {
    await sql.end()
  })

  it("should return the inserted row", async () => {
    await sql`INSERT INTO foo (_id, msg) VALUES (${sql.typed.int(1)}, 'Hello world!')`

    assert.deepStrictEqual([...await sql`SELECT * FROM foo`],
                           [{_id: 1, msg: 'Hello world!'}])
  })
})
