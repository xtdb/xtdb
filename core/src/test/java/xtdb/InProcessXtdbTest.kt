package xtdb

import clojure.lang.Keyword
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import xtdb.InProcessXtdb.startNode
import xtdb.query.Expr.lVar
import xtdb.query.Binding
import xtdb.query.Query.from
import xtdb.tx.Ops.Companion.put

internal class InProcessXtdbTest {
    @Test
    fun startsInMemoryNode() {
        startNode().use { node ->
            node.submitTx(
                listOf(
                    put(Keyword.intern("foo"), mapOf("xt/id" to "jms"))
                )
            )

            Assertions.assertEquals(
                listOf(mapOf(Keyword.intern("id") to "jms")),

                node.query(
                    from("foo")
                        .binding(listOf(Binding("xt/id", lVar("id"))))
                )
            )
        }
    }
}
