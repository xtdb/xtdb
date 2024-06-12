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
                client.submitTx(sql("INSERT INTO foo (xt\$id) VALUES ('jms')"))

                assertEquals(
                    listOf(mapOf("id" to "jms")),

                    client.openQuery("SELECT xt\$id AS id FROM foo").use { it.toList() }
                )

                assertEquals(
                    listOf(mapOf("foo_id" to "jms")),

                    client.openQuery("SELECT foo.xt\$id AS foo_id FROM foo").use { it.toList() }
                )

                assertEquals(
                    listOf(mapOf("foo_id" to "jms")),

                    client.openQuery("SELECT foo.xt\$id AS foo_id FROM foo").use { it.toList() }
                )
            }
        }
    }
}
