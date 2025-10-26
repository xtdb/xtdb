---
title: Using XTDB from C
---

In C, you can talk to a running XTDB node using [libpq](https://www.postgresql.org/docs/current/libpq.html), the PostgreSQL C library, taking advantage of XTDB's PostgreSQL wire-compatibility.

## Install

On most Linux distributions, you can install libpq via your package manager:

**Ubuntu/Debian:**
```bash
sudo apt-get install libpq-dev
```

**Fedora/RHEL:**
```bash
sudo dnf install libpq-devel
```

**macOS (Homebrew):**
```bash
brew install libpq
```

## Connect

Once you've [started your XTDB node](/intro/installation-via-docker), you can use the following code to connect to it:

```c
#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>

int main() {
    PGconn *conn = PQconnectdb("host=localhost port=5432 dbname=xtdb user=xtdb");

    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        exit(1);
    }

    // Insert records
    PGresult *res = PQexec(conn,
        "INSERT INTO users RECORDS {_id: 'alice', name: 'Alice'}, {_id: 'bob', name: 'Bob'}");

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Insert failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }
    PQclear(res);

    // Query records
    res = PQexec(conn, "SELECT _id, name FROM users");

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Query failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }

    printf("Users:\n");
    int rows = PQntuples(res);
    for (int i = 0; i < rows; i++) {
        char *id = PQgetvalue(res, i, 0);
        char *name = PQgetvalue(res, i, 1);
        printf("  * %s: %s\n", id, name);
    }

    PQclear(res);
    PQfinish(conn);

    printf("\n✓ XTDB connection successful\n");
    return 0;
}

/* Output:

Users:
  * alice: Alice
  * bob: Bob

✓ XTDB connection successful
*/
```

## Compilation

To compile your C program with libpq:

```bash
gcc -o xtdb_example xtdb_example.c -lpq
```

## Examples

For more examples and tests, see the [XTDB driver-examples repository](https://github.com/xtdb/driver-examples), which contains comprehensive test suites demonstrating various features and use cases.
