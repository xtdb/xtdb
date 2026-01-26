---
title: Using XTDB from JavaScript
---

In NodeJS, you can talk to a running XTDB node using the [postgres](https://www.npmjs.com/package/postgres) package, taking advantage of XTDB's PostgreSQL wire-compatibility.

This is a basic configuration for XTDB interoperability:

``` javascript
"use strict"

import postgres from 'postgres';

const OID = {
  boolean: 16,
  int64: 20,
  int32: 23,
  text: 25,
  float64: 701,
  transit: 16384,
};

const sql = postgres({
  host: "localhost",
  port: 5432,
});

async function main() {
    await sql`
      INSERT INTO users (_id, name) VALUES
      (${sql.typed(1, OID.int32)}, ${sql.typed("James", OID.text)}),
      (${sql.typed(2, OID.int32)}, ${sql.typed("Jeremy", OID.text)})
    `;

    console.log([...(await sql`SELECT _id, name FROM users`)]);
    // => [ { _id: 2, name: 'Jeremy' }, { _id: 1, name: 'James' } ]
}

main();
```

## Specifying the data type of parameters

XTDB learns your DB schema as you enter data. It doesn't provide a way for specifying and enforcing a schema in advance.

Therefore, you will want to choose a PostgreSQL client that supports specifying the data type of inserted columns. This is done in PostgreSQL through the use of [OIDs](https://www.postgresql.org/docs/current/datatype-oid.html), which are just numbers that identify a DB data type.

As shown in the example above, the [postgres](https://www.npmjs.com/package/postgres) package does support specifying OIDs for types. (At the time of this writing, that is not the case for the [pg](https://www.npmjs.com/package/pg) package).

## Ensuring parameters and returned records are fully typed

XTDB supports `transit`, which is a structured data format with typed values.

This is a sample configuration of `Postgres.js` for transit support:

``` javascript
import transit from "transit-js";


const transitReader = transit.reader("json");
const transitWriter = transit.writer("json");

const sql = postgres({
  ...,

  connection: {
    // Record objects will be returned fully typed using the transit format:
    // Options: "json" (default, simple strings), "json-ld" (structured with @type/@value), "transit"
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

    // By default, int64 values are handled as text.
    // Reading int64 values as a number, ensuring no loss of precision:
    int64: {
      from: [20],
      parse: (x) => {
        const res = parseInt(x);
        if (!Number.isSafeInteger(res))
          throw Error(`Could not convert to integer reliably: ${x}`);
        return res;
      },
    } /*as unknown as postgres.PostgresType*/, // for TypeScript
  },
});
```

The above configuration allows to pass record parameters as fully typed objects:
``` javascript
await sql`
  INSERT INTO users (_id, name) RECORDS
    ${sql.types.transit({ _id: 1, name: "James" })},
    ${sql.types.transit({ _id: 2, name: "Jeremy" })}
`;
```

## Examples

For more examples and tests, see the [XTDB driver-examples repository](https://github.com/xtdb/driver-examples), which contains comprehensive test suites demonstrating various features and use cases.
