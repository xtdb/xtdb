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

## Examples

For more examples and tests, see the [XTDB driver-examples repository](https://github.com/xtdb/driver-examples), which contains comprehensive test suites demonstrating various features and use cases.
