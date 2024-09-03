package xtdb.api

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.Xtdb.openNode
import xtdb.api.query.Queries.from
import xtdb.api.tx.TxOps.putDocs
import xtdb.api.tx.TxOps.sql
import java.net.URI

internal class XtdbClientTest {
    @Test
    fun startsRemoteNode() {
        openNode { httpServer() }.use { _ ->
            XtdbClient.openClient(URI("http://localhost:3000").toURL()).use { client ->
                client.submitTx(sql("INSERT INTO foo (_id) VALUES ('jms')"))

                assertEquals(
                    listOf(mapOf("id" to "jms")),

                    client.openQuery("SELECT _id AS id FROM foo").use { it.toList() }
                )

                assertEquals(
                    listOf(mapOf("foo_id" to "jms")),

                    client.openQuery("SELECT foo._id AS foo_id FROM foo").use { it.toList() }
                )

                assertEquals(
                    listOf(mapOf("foo_id" to "jms")),

                    client.openQuery("SELECT foo._id AS foo_id FROM foo").use { it.toList() }
                )
            }
        }
    }
}
