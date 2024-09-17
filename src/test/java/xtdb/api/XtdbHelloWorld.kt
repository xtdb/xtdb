package xtdb.api

import java.sql.DriverManager

fun main() {
    DriverManager.getConnection("jdbc:postgresql://localhost:5432/xtdb").use { connection ->
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