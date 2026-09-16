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
import xtdb.storage.MemoryStorage
import java.time.Instant

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
}
