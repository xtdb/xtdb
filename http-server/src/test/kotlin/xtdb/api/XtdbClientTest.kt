package xtdb.api

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.Xtdb.openNode
import xtdb.api.query.Query.Companion.from
import xtdb.api.tx.TxOp.Companion.put
import java.net.URL

internal class XtdbClientTest {
    @Test
    fun startsRemoteNode() {
        openNode { httpServer() }.use { _ ->
            XtdbClient.openClient(URL("http://localhost:3000")).use { client ->
                client.submitTx(put("foo", mapOf("xt/id" to "jms")))

                assertEquals(
                    listOf(mapOf("id" to "jms")),

                    client.openQuery(
                        from("foo") { "xt/id" boundTo "id" }
                    ).use { it.toList() }
                )

                assertEquals(
                    listOf(mapOf("foo_id" to "jms")),

                    client.openQuery("SELECT foo.xt\$id AS foo_id FROM foo").use { it.toList() }
                )
            }
        }
    }
}
