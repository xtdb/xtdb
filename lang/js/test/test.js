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
        int4: { to: 23 },
	bool: {to: 16}
      }
    })
  })

  after(async () => {
    await sql.end()
  })

  it("should return the inserted row", async () => {
    await sql`INSERT INTO foo (_id, msg) VALUES (${sql.typed.int4(1)}, 'Hello world!')`

    assert.deepStrictEqual([...await sql`SELECT * FROM foo`],
                           [{_id: 1, msg: 'Hello world!'}])
  })

  /*it("JSON-like types can be roundtripped", async () => {
    await sql`INSERT INTO foo2 (_id, bool) VALUES (1, ${sql.typed.bool(true)})`

    assert.deepStrictEqual([...await sql`SELECT * FROM foo2`],
                           [{_id: '1', bool: true}])
  })*/

})
