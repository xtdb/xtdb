package xtdb.api

import clojure.lang.Keyword
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.query.Query.Companion.from
import xtdb.api.query.toVar
import xtdb.api.tx.TxOp.Companion.put
import java.net.URL

internal class XtdbClientTest {
    @Test
    fun startsRemoteNode() {
        XtdbServer.startServer().use {
            XtdbClient.startClient(URL("http://localhost:9832")).use { node ->
                node.submitTx(put("foo", mapOf("xt/id" to "jms")))

                assertEquals(
                    listOf(mapOf("id" to "jms")),

                    node.openQuery(
                        from("foo", "xt/id" toVar "id")
                    ).use { it.toList() }
                )

                assertEquals(
                    listOf(mapOf("foo_id" to "jms")),

                    node.openQuery("SELECT foo.xt\$id AS foo_id FROM foo").use { it.toList() }
                )
            }
        }
    }
}
