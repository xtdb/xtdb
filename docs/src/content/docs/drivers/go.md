---
title: Using XTDB from Go
---

In Go, you can talk to a running XTDB node using the [pgx](https://github.com/jackc/pgx) PostgreSQL driver, taking advantage of XTDB's PostgreSQL wire-compatibility.

## Install

To install the pgx driver, use:

```bash
go get github.com/jackc/pgx/v5
```

## Connect

Once you've [started your XTDB node](/intro/installation-via-docker), you can use the following code to connect to it:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
)

func main() {
	conn, err := pgx.Connect(context.Background(), "postgres://xtdb:5432/xtdb")
	if err != nil {
		log.Fatalf("Unable to connect: %v\n", err)
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(),
		"INSERT INTO go_users RECORDS {_id: 'alice', name: 'Alice'}, {_id: 'bob', name: 'Bob'}")
	if err != nil {
		log.Fatalf("Insert failed: %v\n", err)
	}

	rows, err := conn.Query(context.Background(), "SELECT _id, name FROM go_users")
	if err != nil {
		log.Fatalf("Query failed: %v\n", err)
	}
	defer rows.Close()

	fmt.Println("Users:")
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatalf("Scan failed: %v\n", err)
		}
		fmt.Printf("  * %s: %s\n", id, name)
	}

	fmt.Println("\n✓ XTDB connection successful")
}

/* Output:

Users:
  * alice: Alice
  * bob: Bob

✓ XTDB connection successful
*/
```

## Arrow-native access via ADBC

For Arrow-native workloads (query results as Arrow batches, bulk-ingesting an Arrow table in one round trip), XTDB also exposes [ADBC](https://arrow.apache.org/adbc/) over its FlightSQL listener.
From Go that's the [`flightsql`](https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc/driver/flightsql) ADBC driver, pointed at `grpc://localhost:9832`:

```go
import (
    "github.com/apache/arrow-adbc/go/adbc"
    "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
    "github.com/apache/arrow/go/v18/arrow/memory"
)

drv := flightsql.NewDriver(memory.DefaultAllocator)
db, _ := drv.NewDatabase(map[string]string{
    adbc.OptionKeyURI: "grpc://localhost:9832",
})
conn, _ := db.Open(ctx)
defer conn.Close()
```

See the [ADBC reference](/adbc/reference) for the supported surface and the [Apache ADBC driver matrix](https://arrow.apache.org/adbc/current/driver/flight_sql.html) for install details.

## Examples

For more examples and tests, see the [XTDB driver-examples repository](https://github.com/xtdb/driver-examples), which contains comprehensive test suites demonstrating various features and use cases.
