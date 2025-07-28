import assert from 'assert';
import postgres from 'postgres';
import tjs from 'transit-js';
import * as uuid from 'uuid';

const transitReadHandlers = {
  'time/zoned-date-time': (s) => new Date(s.replace(/\[.+\]$/, '')),
}

let sql;

beforeEach (async () => {
  sql = postgres({
    host: process.env.PG_HOST || "localhost",
    port: process.env.PG_PORT || 5439,
    database: uuid.v4().toString(),
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
        serialize: (v) => tjs.writer('json').write(v),
        parse: (v) => tjs.reader('json', { handlers: transitReadHandlers }).read(v)
      }
    }
  })
})

afterEach(async () => {
  await sql.end()
})

describe("connects to XT", function() {
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
      await conn`SET fallback_output_format='transit'`

      const ts = new Date('2020-01-01')

      await conn`INSERT INTO foo (_id, transit) VALUES (${conn.typed.int(2)}, ${conn.typed.transit({ts})})`

      const res = await conn`SELECT _id, transit FROM foo WHERE _id = 2`

      // HACK can't figure out how to get it doing this automatically
      res[0].transit = tjs.mapToObject(res[0].transit)

      assert.deepStrictEqual([...res],
                             [{_id: 2, transit: { ts }}])

      assert.deepStrictEqual([...await conn`SELECT _id, (transit).ts + INTERVAL 'P2D' AS third FROM foo WHERE _id = 2`],
                             [{_id: 2, third: new Date("2020-01-03T00:00Z")}])

    } finally {
      await conn.release()
    }
  })

  it("accepts tagged numbers as floats/ints", async () => {
    const conn = await sql.reserve()

    try {
      await conn`INSERT INTO tagged_nums RECORDS {_id: 1, nest: ${conn.typed.transit({a: 1, b: 1.0, c: 1.1, d: tjs.tagged('f64', 1)})}}`

      let res = await conn`SELECT _id, nest FROM tagged_nums`

      assert.deepStrictEqual([...res], [{_id: 1, nest: {a: 1, b: 1, c: 1.1, d: 1}}])

      res = await conn`select * from information_schema.columns WHERE table_name = 'tagged_nums' AND column_name = 'nest'`
      const type = res[0].data_type;
      assert.equal('[:struct {a :i64, b :i64, c :f64, d :f64}]', type, `data_type is actually ${type}`);
    } finally {
      await conn.release()
    }
  })

  it("should round-trip top-level transit via RECORDS", async () => {
    const conn = await sql.reserve()

    try {
      await conn`SET fallback_output_format='transit'`

      const m = {'_id': 2, transit: {'d': new Date('2020-01-01')}}

      await conn`INSERT INTO top_level_records RECORDS ${conn.typed.transit(m)}`

      const res = await conn`SELECT _id, transit FROM top_level_records WHERE _id = 2`

      // HACK can't figure out how to get it doing this automatically
      res[0].transit = tjs.mapToObject(res[0].transit)

      assert.deepStrictEqual([...res], [m])

      assert.deepStrictEqual([...await conn`SELECT _id, (transit).d + INTERVAL 'P2D' AS third FROM top_level_records WHERE _id = 2`],
                             [{_id: 2, third: new Date("2020-01-03T00:00Z")}])

    } finally {
      await conn.release()
    }
  })
})

describe("XT cleans up portals correctly", function() {

  it("closes unamed portal when its rebound", async () => {
    const sql2 = await sql.reserve();
    await sql2`BEGIN TRANSACTION READ ONLY`;
    await sql2`SELECT ${2}`;
    await sql2`COMMIT`;
    // lib uses extended flow by default, commit therefore was being bound as unamed portal
    // ref to previous portal was being lost and therefore never closed
    sql2.release();
    sql.end();
  })

  it("implictly closes unamed portal at start of simple query", async () => {
    const sql2 = await sql.reserve();
    await sql2`BEGIN TRANSACTION READ ONLY`;
    await sql2`SELECT ${2}`;
    await sql2`SELECT 3`.simple();
    sql2.release();
    sql.end();
  })
})

describe("Postgres.js handles cached-query-must-not-change-result-type errors", function() {
  it("re-prepares when outside of a transaction", async () => {
    const conn = await sql.reserve();

    try {
      await conn`INSERT INTO foo (_id, a, b) VALUES (1, 1, 2)`;
      assert.deepStrictEqual([{_id: 1, a: 1, b: 2}], [...await conn`SELECT * FROM foo ORDER BY _id`]);

      await conn`INSERT INTO foo (_id, a, b) VALUES (2, '1', 2)`;

      assert.deepStrictEqual([{_id: 1, a: 1, b: 2}, {_id: 2, a: '1', b: 2}], [...await conn`SELECT * FROM foo ORDER BY _id`])
    } finally {
      conn.release();
    }
  })
})

describe("Postgres.js handles params in BEGIN", function() {
  it("allows params in BEGIN", async () => {
    // Needed for begin
    await sql`SELECT 1`;
    const conn = await sql.reserve();

    try {
      await conn`BEGIN READ ONLY WITH (TIMEZONE = ${"Australia/Perth"})`
      assert.deepStrictEqual([{timezone: 'Australia/Perth'}], [...await conn`SHOW TIME ZONE`]);
      await conn`COMMIT`;
    } finally {
      await conn.release()
    }
  })
})

// FIXME: test case for #4550, bug looks like it's upstream.
// see that card for more info

// describe("INSERT...RECORDS with _system_from should fail, #4550", function() {
//   it("should throw error when using _system_from in RECORDS", async () => {
//     const conn = await sql.reserve();

//     try {
//       const record1 = { _id: 1, v: 1 };
//       await conn`INSERT INTO system RECORDS ${conn.typed.transit(record1)}`;
//       const result = await conn`SELECT * FROM system`;
//       assert.deepStrictEqual([{_id: 1, v: 1}], [...result]);

//       await conn`INSERT INTO system RECORDS {_id: 2, v: 1, _system_from: DATE '2024-01-01'}`;
//       await assert.rejects(
//         async () => {
//           await conn`INSERT INTO system RECORDS {_id: 2, v: 1, _system_from: DATE '2024-01-01'}`;
//         },
//         /Cannot put documents with columns.*_system_from/
//       );

//       assert.deepStrictEqual([{_id: 1, v: 1}], [...await conn`SELECT * FROM system`]);

//       const record2 = {
//         _id: 3,
//         v: '1',
//         _system_from: new Date('2024-01-01T00:00:00'),
//       };

//       await assert.rejects(
//         async () => {
//           await conn`INSERT INTO system RECORDS ${conn.typed.transit(record2)}`;
//         },
//         /Cannot put documents with columns.*_system_from/
//       );
//     } finally {
//       await conn.release()
//     }
//   })
// })
