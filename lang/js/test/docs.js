import postgres from "postgres";
import * as uuid from "uuid";
import transit from "transit-js";
import assert from "assert";

describe("code examples", function () {
  const OID = {
    boolean: 16,
    int64: 20,
    int32: 23,
    text: 25,
    float64: 701,
    transit: 16384,
  };

  const commonPostgresOptions = () => ({
    host: process.env.PG_HOST || "localhost",
    port: process.env.PG_PORT || 5439,
    database: uuid.v4().toString(),
  });

  it("example 1", async () => {
    const sql = postgres(commonPostgresOptions());

    await sql`
      INSERT INTO users (_id, name) VALUES
      (${sql.typed(1, OID.int32)}, ${sql.typed("James", OID.text)}),
      (${sql.typed(2, OID.int32)}, ${sql.typed("Jeremy", OID.text)})
    `;

    console.log([...(await sql`SELECT _id, name FROM users`)]);
    // => [ { _id: 2, name: 'Jeremy' }, { _id: 1, name: 'James' } ]

    assert.deepStrictEqual(
      [...(await sql`SELECT _id, name FROM users ORDER BY _id`)],
      [
        { _id: 1, name: "James" },
        { _id: 2, name: "Jeremy" },
      ],
    );
  });

  it("example 2", async () => {
    const transitReader = transit.reader("json");
    const transitWriter = transit.writer("json");

    const sql = postgres({
      ...commonPostgresOptions(),

      connection: {
        // Record objects will be returned fully typed using the transit format:
        fallback_output_format: "transit",
      },

      types: {
        // Add support for the transit format:
        transit: {
          to: 16384,
          from: [16384],
          serialize: (v) => transitWriter.write(v),
          parse: (v) => transitReader.read(v),
        },

        // By default, Postgres.js reads int64 as text.
        // Reading as JS number, ensuring no loss of precision:
        int64: {
          from: [20],
          parse: (x) => {
            const res = parseInt(x);
            if (!Number.isSafeInteger(res))
              throw Error(`Could not convert to number: ${x}`);
            return res;
          },
        } /*as unknown as postgres.PostgresType*/, // [add for TypeScript]
      },
    });

    await sql`
      INSERT INTO users (_id, name) RECORDS
        ${sql.types.transit({ _id: 1, name: "James" })},
        ${sql.types.transit({ _id: 2, name: "Jeremy" })}
    `;

    console.log([...(await sql`SELECT _id, name FROM users`)]);

    assert.deepStrictEqual(
      [...(await sql`SELECT _id, name FROM users ORDER BY _id`)],
      [
        { _id: 1, name: "James" },
        { _id: 2, name: "Jeremy" },
      ],
    );
  });
});

// Further code excerpts that could be added to docs in the future:
//
//     const transitReader = transit.reader("json", {
//       mapBuilder: {
//         init: () => ({}),
//         finalize: (m) => m,
//         add: (m, k, v) => ({ ...m, [k]: v }),
//       },
//       handlers: {
//         "time/zoned-date-time": (rep) => {
//           // We receive "2024-11-06T11:43:20.123Z[UTC]"
//           // See https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#ISO_ZONED_DATE_TIME
//           const withoutTz = rep.replace(/\[[^\]]*]$/, "");
//           return new Date(withoutTz);
//         },
//         "time/duration": (rep) => rep,
//       },
//     });
