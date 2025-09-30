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
