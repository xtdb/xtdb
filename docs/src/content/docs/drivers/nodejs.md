---
title: Using XTDB from JavaScript
---

In NodeJS, you can talk to a running XTDB node using the ['postgres'](https://www.npmjs.com/package/postgres) package, taking advantage of XTDB's PostgreSQL wire-compatibility.

``` javascript
"use strict"

import postgres from 'postgres';

const sql = postgres({
  host: "localhost",
  port: 5432,
  fetch_types: false, // currently required https://github.com/xtdb/xtdb/issues/3607
  types: {
    bool: {to: 16},
    int: {
        to: 20,
        from: [23, 20], // int4, int8
        parse: parseInt
    }
  }
});

async function main() {
    await sql`INSERT INTO users (_id, name) VALUES (${sql.typed.int(1)}, 'James'), (${sql.typed.int(2)}, 'Jeremy')`

    console.log([...await sql`SELECT _id, name FROM users`])
    // => [{_id: 1, name: "James"}, [{_id: 2, name: "Jeremy"}]]
}

main();
```
