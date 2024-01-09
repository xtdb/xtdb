package xtdb.api

import clojure.lang.Keyword
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.query.Query.Companion.from
import xtdb.query.toVar
import xtdb.tx.TxOp.Companion.put
import java.net.URL

internal class XtdbClientTest {
    @Test
    fun startsRemoteNode() {
        XtdbServer.startServer().use {
            XtdbClient.startClient(URL("http://localhost:9832")).use { node ->
                node.submitTx(put("foo", mapOf("xt/id" to "jms")))

                assertEquals(
                    listOf(mapOf(Keyword.intern("id") to "jms")),

                    node.openQuery(
                        from("foo")
                            .binding(listOf("xt/id" toVar "id"))
                    ).use { it.toList() }
                )

                assertEquals(
                    listOf(mapOf(Keyword.intern("foo_id") to "jms")),

                    node.openQuery("SELECT foo.xt\$id AS foo_id FROM foo").use { it.toList() }
                )
            }
        }
    }
}
