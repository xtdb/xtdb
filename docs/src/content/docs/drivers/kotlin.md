---
title: Using XTDB from Kotlin
---

In Kotlin, you can talk to a running XTDB node using standard [Java JDBC](https://docs.oracle.com/javase/tutorial/jdbc/overview/) tooling, using XTDB's Postgres wire-compatibility.

## Install

To install the XTDB JDBC driver, add the following dependency to your Gradle `build.gradle.kts`:

``` kotlin
// https://mvnrepository.com/artifact/com.xtdb/xtdb-api

implementation("com.xtdb:xtdb-api:$XTDB_VERSION")
```

Or, for Maven:

``` xml
<dependency>
    <groupId>com.xtdb</groupId>
    <artifactId>xtdb-api</artifactId>
    <version>$XTDB_VERSION</version>
</dependency>
```

## Connect

Once you've [started your XTDB node](/intro/installation-via-docker), you can use the following code to connect to it:

``` kotlin
import java.sql.DriverManager

// This is using relatively raw JDBC - you can also use standard connection pools
// and JDBC abstraction libraries.

fun main() {
    DriverManager.getConnection("jdbc:xtdb://localhost:5432/xtdb").use { connection ->
        connection.createStatement().use { statement ->
            statement.execute("INSERT INTO users RECORDS {_id: 'jms', name: 'James'}, {_id: 'joe', name: 'Joe'}")

            statement.executeQuery("SELECT * FROM users").use { rs ->
                println("Users:")

                while (rs.next()) {
                    println("  * ${rs.getString("_id")}: ${rs.getString("name")}")
                }
            }
        }
    }
}

/* Output:

Users:
  * jms: James
  * joe: Joe
*/
```

## Arrow-native access via ADBC

For Arrow-native workloads (query results as Arrow batches, bulk-ingesting an Arrow table in one round trip), XTDB exposes [ADBC](https://arrow.apache.org/adbc/) both in-process and over FlightSQL.

In-process is the zero-copy path: `node.connect()` returns an `org.apache.arrow.adbc.core.AdbcConnection`, so anything written against the ADBC Java API works against it directly with no network hop:

```kotlin
Xtdb.openNode().use { node ->
    node.connect().use { conn ->
        val stmt = conn.createStatement()
        stmt.setSqlQuery("SELECT 1")
        stmt.executeQuery().use { result ->
            // result.reader is an org.apache.arrow.vector.ipc.ArrowReader
        }
    }
}
```

The same node also serves the [FlightSQL listener](/adbc/reference#over-the-wire-flightsql), so you can reach it from the Apache ADBC Java FlightSQL client over the wire instead.
One caveat there: that client doesn't query `getSessionOptions`, so `getCurrentCatalog` / `getCurrentDbSchema` are unsupported over the wire (an upstream gap, not an XTDB one). The in-process path is unaffected.

See the [ADBC reference](/adbc/reference) for the full supported surface (its examples are in Kotlin).

## Examples

For more examples and tests, see the [XTDB driver-examples repository](https://github.com/xtdb/driver-examples), which contains comprehensive test suites demonstrating various features and use cases.
