package xtdb.api

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.DriverManager

class XtdbHelloWorld {
    data class User(val id: String, val name: String)

    @Test
    fun `hello world`() {
        Xtdb.openNode(Xtdb.Config(ServerConfig(0))).use { xtdb ->
            DriverManager.getConnection("jdbc:postgresql://localhost:${xtdb.serverPort}/xtdb?user=anonymous").use { conn ->
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
}