---
title: Using XTDB from Python
---

In Python, you can talk to a running XTDB node using the standard [psycopg](https://www.psycopg.org) tooling, taking advantage of XTDB's Postgres wire-compatibility.

``` python
import asyncio
import psycopg as pg

DB_PARAMS = {
    "host": "localhost",
    "port": 5432
}

async def insert_trades(conn, trades):
    query = """
    INSERT INTO trades (_id, name, quantity) VALUES (%s, %s, %s)
    """

    async with conn.cursor() as cur:
        for trade in trades:
            trade_values = (trade["_id"], trade["name"], trade["quantity"])
            await cur.execute(query, trade_values)

async def get_trades_over(conn, quantity):
    query = """
    SELECT * FROM trades WHERE quantity > %s
    """
    async with conn.cursor() as cur:
        await cur.execute(query, (quantity,))
        return await cur.fetchall()

async def main():
    trades = [
        {"_id": 1, "name": "Trade1", "quantity": 1001},
        {"_id": 2, "name": "Trade2", "quantity": 15},
        {"_id": 3, "name": "Trade3", "quantity": 200},
    ]

    try:
        async with await pg.AsyncConnection.connect(**DB_PARAMS, autocommit=True) as conn:
            # required for now https://github.com/xtdb/xtdb/issues/3589
            conn.adapters.register_dumper(str, pg.types.string.StrDumperVarchar)
            await insert_trades(conn, trades)
            print("Trades inserted successfully")

            result = await get_trades_over(conn, 100)
            print(result)
    except Exception as error:
        print(f"Error occurred: {error}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Examples

For more examples and tests, see the [XTDB driver-examples repository](https://github.com/xtdb/driver-examples), which contains comprehensive test suites demonstrating various features and use cases.
