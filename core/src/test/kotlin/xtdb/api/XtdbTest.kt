package xtdb.api

import clojure.lang.Keyword
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.query.Binding
import xtdb.query.Expr
import xtdb.query.Expr.Companion.lVar
import xtdb.query.Query
import xtdb.query.Query.Companion.from
import xtdb.tx.TxOp
import xtdb.tx.TxOp.Companion.put

infix fun String.toVar(`var`: String) = Binding(this, lVar( `var`))

internal class XtdbTest {
    @Test
    fun startsInMemoryNode() {
        Xtdb.startNode().use { node ->
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
