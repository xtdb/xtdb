@file:OptIn(xtdb.InternalApi::class)

package xtdb.api.tx

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.NodeBase
import xtdb.NodeBase.Companion.openBase
import xtdb.TestPartition
import xtdb.api.TransactionKey
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector
import xtdb.arrow.VectorType.Companion.IID
import xtdb.arrow.VectorType.Companion.INSTANT
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.storage.MemoryStorage
import xtdb.time.InstantUtil.asMicros
import xtdb.time.microsAsInstant
import xtdb.util.asIid
import java.time.Instant
import java.util.UUID

class OpenTxTest {

    private lateinit var nodeBase: NodeBase
    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        nodeBase = openBase(openMeterRegistry = false)
        allocator = nodeBase.allocator.newChildAllocator("test", 0, Long.MAX_VALUE)
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
        nodeBase.close()
    }

    private fun <R> withOpenTx(dbName: String, f: (OpenTx) -> R): R =
        MemoryStorage(allocator, epoch = 0).use { bp ->
            TestPartition(allocator, bp).use { partition ->
                OpenTx(
                    allocator, nodeBase, partition.storage, partition.state, dbName,
                    TransactionKey(0, Instant.EPOCH), null
                ).use(f)
            }
        }

    @Test
    fun `query catalog contains only the tx's own database`() {
        withOpenTx("my_db") { tx ->
            val queryCatalog = tx.queryCatalog

            assertEquals(setOf("my_db"), queryCatalog.databaseNames.toSet())
            assertEquals("my_db", queryCatalog.databaseOrNull("my_db")?.name)
            assertNull(queryCatalog.databaseOrNull("other_db"))
        }
    }

    @Test
    fun `batch puts and deletes over a slice take each row's id and valid times, defaulting null and absent ones`() {
        fun t(day: Int) = Instant.parse("2021-01-%02dT00:00:00Z".format(day)).asMicros
        val defaultVf = t(1) - 1
        val ids = listOf(0L, "one", 2L, UUID(0, 3), "four")

        val putCols = listOf(
            Vector.fromList(allocator, "_id", ids),
            Vector.fromList(allocator, "doc", List(5) { mapOf("v" to it.toLong()) }),
            Vector.fromList(allocator, "_valid_from", maybe(INSTANT), listOf(t(1), null, t(3), null, t(5)).map { it?.microsAsInstant }),
        )

        val deleteCols = listOf(
            Vector.fromList(allocator, "_iid", IID, ids.map { it.asIid }),
            Vector.fromList(allocator, "_valid_to", maybe(INSTANT), listOf(null, t(2), null, t(4), null).map { it?.microsAsInstant }),
        )

        try {
            withOpenTx("my_db") { tx ->
                val table = tx.table(tableName = "docs")
                table.writePuts(RelationReader.from(putCols).select(1, 3), defaultVf, Long.MAX_VALUE)
                table.writeDeletes(RelationReader.from(deleteCols).select(2, 2), defaultVf, Long.MAX_VALUE)

                val rel = table.tx.relation
                val iidVec = rel["_iid"]
                val vfVec = rel["_valid_from"]
                val vtVec = rel["_valid_to"]

                assertEquals(
                    listOf(
                        Triple(ids[1], defaultVf, Long.MAX_VALUE),
                        Triple(ids[2], t(3), Long.MAX_VALUE),
                        Triple(ids[3], defaultVf, Long.MAX_VALUE),
                        Triple(ids[2], defaultVf, Long.MAX_VALUE),
                        Triple(ids[3], defaultVf, t(4)),
                    ).map { (id, vf, vt) -> Triple(id.asIid.toList(), vf, vt) },
                    List(rel.rowCount) { idx ->
                        val iid = iidVec.getBytes(idx).let { bb -> ByteArray(bb.remaining()).also { bb.duplicate().get(it) } }
                        Triple(iid.toList(), vfVec.getLong(idx), vtVec.getLong(idx))
                    }
                )
            }
        } finally {
            putCols.forEach { it.close() }
            deleteCols.forEach { it.close() }
        }
    }
}
