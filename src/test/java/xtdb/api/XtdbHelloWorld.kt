package xtdb.api

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.NodeResolver
import java.sql.DriverManager

@ExtendWith(NodeResolver::class)
class XtdbHelloWorld {
    data class User(val id: String, val name: String)

    @Test
    fun `hello world`(xtdb: Xtdb) {
        xtdb.connection.use { conn ->
            conn.createStatement().use { statement ->
                statement.execute("INSERT INTO users RECORDS {_id: 'jms', name: 'James'}, {_id: 'joe', name: 'Joe'}")

                statement.executeQuery("SELECT * FROM users").use { rs ->

                    val users = mutableListOf<User>()

                    while (rs.next()) {
                        users.add(User(rs.getString("_id"), rs.getString("name")))
                    }

                    assertEquals(listOf(User("joe", "Joe"), User("jms", "James")), users)
                }
            }
        }
    }
}
