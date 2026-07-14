---
title: Using XTDB from Java
---

In Java, you can talk to a running XTDB node using standard [Java JDBC](https://docs.oracle.com/javase/tutorial/jdbc/overview/) tooling, using XTDB's Postgres wire-compatibility.

## Install

To install the XTDB JDBC driver, add the following dependency to your Maven `pom.xml`:

``` xml
<!-- https://mvnrepository.com/artifact/com.xtdb/xtdb-api -->

<dependency>
    <groupId>com.xtdb</groupId>
    <artifactId>xtdb-api</artifactId>
    <version>$XTDB_VERSION</version>
</dependency>
```

Or, for Gradle:

``` kotlin
implementation("com.xtdb:xtdb-api:$XTDB_VERSION")
```

## Connect

Once you've [started your XTDB node](/intro/installation-via-docker), you can use the following code to connect to it:

``` java
import java.sql.DriverManager;
import java.sql.SqlException;

public class XtdbHelloWorld {

  // This is using relatively raw JDBC - you can also use standard connection pools
  // and JDBC abstraction libraries.

  public static void main(String[] args) throws SqlException {
    try (var connection =
           DriverManager.getConnection("jdbc:xtdb://localhost:5432/xtdb", "xtdb", "xtdb");
         var statement = connection.createStatement()) {

      statement.execute("INSERT INTO users RECORDS {_id: 'jms', name: 'James'}, {_id: 'joe', name: 'Joe'}");

      try (var resultSet = statement.executeQuery("SELECT * FROM users")) {
        System.out.println("Users:");

        while (resultSet.next()) {
          System.out.printf("  * %s: %s%n", resultSet.getString("_id"), resultSet.getString("name"));
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      throw e;
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

```java
try (var node = Xtdb.openNode();
     var conn = node.connect();
     var stmt = conn.createStatement()) {
    stmt.setSqlQuery("SELECT 1");
    try (var result = stmt.executeQuery()) {
        // result.getReader() is an org.apache.arrow.vector.ipc.ArrowReader
    }
}
```

The same node also serves the [FlightSQL listener](/adbc/reference#over-the-wire-flightsql), so you can reach it from the Apache ADBC Java FlightSQL client over the wire instead.
One caveat there: that client doesn't query `getSessionOptions`, so `getCurrentCatalog` / `getCurrentDbSchema` are unsupported over the wire (an upstream gap, not an XTDB one). The in-process path is unaffected.

See the [ADBC reference](/adbc/reference) for the full supported surface.

## API Reference

The [Kotlin/Java API documentation](/drivers/kotlin/kdoc) covers the `xtdb-api` and `xtdb-core` node and configuration APIs, along with the storage, log and source modules (S3, Azure Blob Storage, Google Cloud Storage, Kafka, Postgres).

## Examples

For more examples and tests, see the [XTDB driver-examples repository](https://github.com/xtdb/driver-examples), which contains comprehensive test suites demonstrating various features and use cases.
