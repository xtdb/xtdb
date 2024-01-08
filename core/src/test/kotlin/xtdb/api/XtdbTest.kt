package xtdb.api

import clojure.lang.Keyword
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.query.Binding
import xtdb.query.Expr
import xtdb.query.Expr.Companion.lVar
import xtdb.query.Query
import xtdb.query.Query.Companion.from
import xtdb.tx.TxOp.Companion.put

fun Query.From.binding(vararg bindings: Pair<String, Expr>) = binding(bindings.map { Binding(it.first, it.second) })

internal class XtdbTest {
    @Test
    fun startsInMemoryNode() {
        Xtdb.startNode().use { node ->
            node.submitTx(
                listOf(
                    put(Keyword.intern("foo"), mapOf("xt/id" to "jms"))
                )
            )

            assertEquals(
                listOf(mapOf(Keyword.intern("id") to "jms")),

                node.openQuery(
                    from("foo")
                        .binding("xt/id" to lVar("id"))
                ).use { it.toList() }
            )
        }
    }
}
