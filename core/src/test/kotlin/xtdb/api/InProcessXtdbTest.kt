package xtdb.api

import clojure.lang.Keyword
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import xtdb.query.Binding
import xtdb.query.Expr
import xtdb.query.Query
import xtdb.tx.Ops

internal class InProcessXtdbTest {
    @Test
    fun startsInMemoryNode() {
        InProcessXtdb.startNode().use { node ->
            node.submitTx(
                listOf(
                    Ops.put(Keyword.intern("foo"), mapOf("xt/id" to "jms"))
                )
            )

            Assertions.assertEquals(
                listOf(mapOf(Keyword.intern("id") to "jms")),

                node.openQuery(
                    Query.from("foo")
                        .binding(listOf(Binding("xt/id", Expr.lVar("id"))))
                ).use { it.toList() }
            )
        }
    }
}
