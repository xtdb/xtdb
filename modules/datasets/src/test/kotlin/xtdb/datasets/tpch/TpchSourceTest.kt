package xtdb.datasets.tpch

import io.airlift.tpch.TpchColumn
import io.airlift.tpch.TpchColumnType
import io.airlift.tpch.TpchEntity
import io.airlift.tpch.TpchTable
import io.kotest.assertions.nondeterministic.eventually
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import xtdb.XtdbInternal
import xtdb.api.TransactionResult
import xtdb.api.Xtdb
import xtdb.api.error.Incorrect
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.tx.OpenTx
import xtdb.api.tx.TxIndexer
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

class TpchSourceTest {

    // at small scale factors the generator repeats some (partkey, suppkey) pairs, which a put collapses
    @Suppress("UNCHECKED_CAST")
    private fun distinctKeyCount(table: TpchTable<*>, scaleFactor: Double): Long {
        val pkCols = when (table.tableName) {
            "partsupp" -> listOf("ps_partkey", "ps_suppkey")
            "lineitem" -> listOf("l_orderkey", "l_linenumber")
            else -> listOf(table.columns.first().columnName)
        }.map { table.getColumn(it) as TpchColumn<TpchEntity> }

        return (table.createGenerator(scaleFactor, 1, 1) as Iterable<TpchEntity>)
            .distinctBy { e ->
                pkCols.map { if (it.type.base == TpchColumnType.Base.IDENTIFIER) it.getIdentifier(e) else it.getInteger(e).toLong() }
            }
            .size.toLong()
    }

    private fun Xtdb.query(sql: String): List<Map<String, Any?>> =
        createConnectionBuilder().database("tpch").build().use { c ->
            c.createStatement().use { s ->
                s.executeQuery(sql).use { rs ->
                    val cols = (1..rs.metaData.columnCount).map { rs.metaData.getColumnName(it) }
                    buildList { while (rs.next()) add(cols.associateWith { rs.getObject(it) }) }
                }
            }
        }

    @ParameterizedTest
    @ValueSource(ints = [1, 3])
    fun `attaching a !Tpch database loads every table at the scale factor`(threads: Int) = runBlocking {
        Xtdb.openNode().use { node ->
            node.createConnectionBuilder().build().use { c ->
                c.createStatement().use { s ->
                    s.execute(
                        """
                        ATTACH DATABASE tpch WITH $$
                          externalSource: !Tpch
                            scaleFactor: 0.001
                            threads: $threads
                        $$""".trimIndent()
                    )
                }
            }

            eventually(60.seconds) {
                val db = (node as XtdbInternal).dbCatalog["tpch"]
                assert(db?.watchers?.externalSourceToken != null) { "TPC-H load complete" }
            }

            eventually(10.seconds) {
                for (table in TpchTable.getTables()) {
                    assertEquals(
                        distinctKeyCount(table, 0.001),
                        node.query("SELECT COUNT(*) AS n FROM ${table.tableName}").single()["n"],
                        table.tableName,
                    )
                }
            }

            val revenue = node.query(
                """
                SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem
                WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
                  AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24
                """.trimIndent()
            ).single()["revenue"] as Double

            assertEquals(77949.9186, revenue, 1e-6)
        }
    }

    @Test
    fun `a completed load writes nothing on a later term`() = runBlocking {
        val txIndexer = object : TxIndexer {
            override val latestBlock = MutableStateFlow(null)

            override suspend fun executeTx(
                externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
                writer: suspend (OpenTx) -> TxIndexer.TxResult,
            ): TransactionResult = error("wrote a tx")

            override suspend fun submitTx(
                externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
                writer: suspend (OpenTx) -> TxIndexer.TxResult,
            ): Deferred<TransactionResult> = error("wrote a tx")
        }

        TpchSource.Factory(0.001).open("tpch", emptyMap()).use { source ->
            source.onPartitionAssigned(0, "tpch-loaded".toByteArray(), txIndexer)
        }
    }

    @Test
    fun `the factory round-trips through its registration`() {
        val factory = TpchSource.Factory(scaleFactor = 0.5, threads = 4)
        val reg = TpchSource.Factory.Registration()

        assertEquals(factory, reg.fromProto(reg.toProto(factory)))
    }

    @Test
    fun `the factory rejects a non-positive or non-finite scale factor, or a non-positive thread count`() {
        assertThrows<Incorrect> { TpchSource.Factory(scaleFactor = 0.0).open("tpch", emptyMap()) }
        assertThrows<Incorrect> { TpchSource.Factory(scaleFactor = Double.NaN).open("tpch", emptyMap()) }
        assertThrows<Incorrect> { TpchSource.Factory(scaleFactor = Double.POSITIVE_INFINITY).open("tpch", emptyMap()) }
        assertThrows<Incorrect> { TpchSource.Factory(scaleFactor = 0.01, threads = 0).open("tpch", emptyMap()) }
    }
}
