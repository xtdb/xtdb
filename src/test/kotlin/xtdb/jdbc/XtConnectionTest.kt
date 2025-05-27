package xtdb.jdbc

import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.api.Xtdb
import xtdb.test.NodeResolver
import xtdb.time.asInstant
import xtdb.time.asOffsetDateTime
import xtdb.time.asZonedDateTime
import java.sql.Timestamp
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.*
import kotlin.test.assertTrue

@ExtendWith(NodeResolver::class)
class XtConnectionTest {

    private fun Transaction.execForOne(sql: String) = exec(sql) { it.next(); it.getObject(1) }

    private fun <T> Transaction.execForOne(sql: String, c: Class<T>) =
        exec(sql) { it.next(); it.getObject(1, c) }

    private fun Transaction.execForOneTimestamp(sql: String) = exec(sql) { it.next(); it.getTimestamp(1) }

    @Test
    fun testDateStyle(node: Xtdb) {
        transaction(Database.connect(node)) {
            assertEquals(
                "2020-01-01Z".asZonedDateTime(),
                execForOne("SELECT TIMESTAMP '2020-01-01Z' ts")
            )

            assertEquals(
                "2020-01-01T00:00:00Z".asOffsetDateTime(),
                execForOne("SELECT TIMESTAMP '2020-01-01Z' ts", OffsetDateTime::class.java)
            )

            assertEquals(
                Timestamp.from("2020-01-01Z".asInstant()),
                execForOneTimestamp("SELECT TIMESTAMP '2020-01-01Z' ts")
            )

            exec("SET datestyle = 'iso8601'")
            assertEquals(
                "2020-05-01+01:00[Europe/London]".asZonedDateTime(),
                execForOne("SELECT TIMESTAMP '2020-05-01+01:00[Europe/London]' ts")
            )

            assertEquals(
                "2020-05-01-08:00".asZonedDateTime(),
                execForOne("SELECT TIMESTAMP '2020-05-01-08:00' ts")
            )

            exec("SET datestyle = 'iso'")
            assertEquals(
                "2020-05-01-08:00".asZonedDateTime(),
                execForOne("SELECT TIMESTAMP '2020-05-01-08:00' ts")
            )
        }
    }

    @Test
    fun `test j-u-Date is UTC`(node: Xtdb) {
        val now = Date()
        node.getConnection().use { conn ->
            conn.prepareStatement("INSERT INTO foo (_id, dt) VALUES (?, ?)").use { stmt ->
                stmt.setObject(1, "setObject")
                stmt.setObject(2, now)
                stmt.execute()
            }

            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT dt FROM foo").use { res ->
                    assertTrue(res.next())
                    assertEquals(
                        now.toInstant().atZone(ZoneOffset.UTC),
                        res.getObject(1)
                    )
                }
            }
        }
    }
}
