package xtdb.api

import clojure.lang.Keyword
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.query.Binding
import xtdb.query.Expr
import xtdb.query.Query
import xtdb.tx.TxOp

internal class XtdbTest {
    @Test
    fun startsInMemoryNode() {
        Xtdb.startNode().use { node ->
            node.submitTx(
                listOf(
                    TxOp.put(Keyword.intern("foo"), mapOf("xt/id" to "jms"))
                )
            )

            assertEquals(
                listOf(mapOf(Keyword.intern("id") to "jms")),

                node.openQuery(
                    Query.from("foo")
                        .binding(listOf(Binding("xt/id", Expr.lVar("id"))))
                ).use { it.toList() }
            )
        }
    }
}
