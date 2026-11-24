package xtdb.indexer

import clojure.lang.Keyword
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.XtdbInternal
import xtdb.api.TransactionResult
import xtdb.api.Xtdb
import xtdb.api.error.Incorrect
import xtdb.api.log.SourceMessage
import xtdb.arrow.IID_TYPE
import xtdb.arrow.INSTANT_TYPE
import xtdb.arrow.LIST_TYPE
import xtdb.arrow.Relation
import xtdb.arrow.STRUCT_TYPE
import xtdb.arrow.UNION_TYPE
import xtdb.test.NodeResolver
import xtdb.util.asIid
import java.time.Duration
import java.time.ZoneId

@ExtendWith(NodeResolver::class)
class ClientSentPutDocsIidsTest {

    /** A put-docs as a submitter predating #3811 wrote it: each doc's IID hashed from its `_id`. */
    private fun Xtdb.submitWithClientIids(table: String, docs: List<Map<String, Any?>>): TransactionResult? {
        val txOps = RootAllocator().use { al ->
            Relation(al).use { rel ->
                val putVec = rel.vectorFor("tx-ops", UNION_TYPE, false).vectorFor("put-docs", STRUCT_TYPE, false)
                val iidsVec = putVec.vectorFor("iids", LIST_TYPE, true)
                val iidWriter = iidsVec.getListElements(IID_TYPE, false)
                val tableDocsVec = putVec.vectorFor("documents", UNION_TYPE, false).vectorFor("public/$table", LIST_TYPE, false)
                val docWriter = tableDocsVec.getListElements(STRUCT_TYPE, false)

                for (doc in docs) {
                    iidWriter.writeBytes(doc.getValue("_id")!!.asIid)
                    docWriter.writeObject(doc)
                }
                iidsVec.endList()
                tableDocsVec.endList()
                putVec.vectorFor("_valid_from", INSTANT_TYPE, true).writeNull()
                putVec.vectorFor("_valid_to", INSTANT_TYPE, true).writeNull()
                putVec.endStruct()
                rel.endRow()

                rel.toArrowStream()
            }
        }

        val db = (this as XtdbInternal).dbCatalog.primary
        val msgId = db.sourceLog.appendMessageBlocking(SourceMessage.Tx(txOps, null, ZoneId.of("UTC"), null, null)).msgId
        return db.awaitTxBlocking(msgId, Duration.ofSeconds(5))
    }

    private fun Xtdb.exec(sql: String) = getConnection().use { conn -> conn.createStatement().use { it.execute(sql) } }

    private fun Xtdb.rows(sql: String): List<List<Any?>> =
        getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery(sql).use { res ->
                    buildList { while (res.next()) add((1..res.metaData.columnCount).map(res::getObject)) }
                }
            }
        }

    @Test
    fun `client-sent IIDs are ignored for a table keyed on another column`(node: Xtdb) {
        node.exec("CREATE TABLE t (a PRIMARY KEY, b)")

        assertInstanceOf(
            TransactionResult.Committed::class.java,
            node.submitWithClientIids("t", listOf(mapOf("_id" to 99L, "a" to 1L, "b" to "x"))),
        )

        node.exec("INSERT INTO t (a, b) VALUES (1, 'y')")

        assertEquals(listOf(listOf<Any?>(1L, "y")), node.rows("SELECT a, b FROM t"), "the second write upserts the first, on `a`")
    }

    @Test
    fun `a client-sent put missing the table's key aborts with missing-id`(node: Xtdb) {
        node.exec("CREATE TABLE t (a PRIMARY KEY, b)")

        val result = node.submitWithClientIids("t", listOf(mapOf("_id" to 99L, "b" to "x")))

        val error = assertInstanceOf(TransactionResult.Aborted::class.java, result).error
        assertEquals(
            Keyword.intern("xtdb.tx", "missing-id"),
            assertInstanceOf(Incorrect::class.java, error).data.valAt(Keyword.intern("xtdb.error", "code")),
        )
    }
}
