package xtdb.indexer

import com.google.protobuf.ByteString
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.storage.Storage
import xtdb.block.proto.block
import xtdb.catalog.TableCatalog
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.trieMetadata
import xtdb.storage.MemoryStorage
import xtdb.test.AllocatorResolver
import java.time.Instant

@ExtendWith(AllocatorResolver::class)
class OffloadedMessagesTest {

    /** Declines anything over [maxBytes], the way a Kafka log declines a record over `max.request.size`. */
    private class DecliningDriver(private val maxBytes: Int) : LogProcessor.LogsDriver {
        val appended = mutableListOf<ReplicaMessage>()

        override suspend fun enqueueToReplica(msg: ReplicaMessage): Deferred<Log.MessageMetadata> {
            if (msg.encode().size > maxBytes)
                throw Log.MessageTooLargeException("${msg.encode().size} bytes is over $maxBytes")

            appended += msg
            return CompletableDeferred(Log.MessageMetadata(0, appended.size - 1L, Instant.EPOCH))
        }

        override suspend fun requestFlushBlock(expectedBlockIdx: Long) = error("unused")
    }

    private fun bigTriesAdded(termId: Long = 3) =
        ReplicaMessage.TriesAdded(
            Storage.VERSION, 0,
            (0 until 64).map { i ->
                TrieDetails.newBuilder()
                    .setTableName("table-$i")
                    .setTrieKey("trie-key-$i")
                    .setTrieMetadata(trieMetadata { iidBloom = ByteString.copyFrom(ByteArray(8 * 1024)) })
                    .build()
            },
            sourceMsgId = 12, termId = termId,
        )

    private fun offloading(bufferPool: MemoryStorage, delegate: LogProcessor.LogsDriver, blockIndex: Long?) =
        OffloadingLogsDriver(
            delegate,
            PartitionStorage(DatabaseLogs(null, null), bufferPool, null),
            PartitionState(blockIndex?.let { TableCatalog(bufferPool, block { this.blockIndex = it }) }, null, null),
        )

    @Test
    fun `a declined message is offloaded, and reads back as the message it stood in for`(al: BufferAllocator) =
        runTest {
            MemoryStorage(al, 0).use { bufferPool ->
                val declining = DecliningDriver(maxBytes = 1024)
                val declined = bigTriesAdded().withTermSeq(4)

                offloading(bufferPool, declining, blockIndex = 7).appendToReplica(declined)

                val ref = assertInstanceOf(ReplicaMessage.OversizedMessage::class.java, declining.appended.single())
                assertEquals(declined.termId, ref.termId, "the envelope carries the declined message's term")
                assertEquals(declined.termSeq, ref.termSeq, "and its position in that term")
                assertEquals(8L, ref.blockIndex, "keyed under the open block, not the last completed one")

                val record = Log.Record(0, 0, Instant.EPOCH, ref as ReplicaMessage)
                assertEquals(
                    declined, bufferPool.resolveOversized(record).message,
                    "resolves back to the message that was declined",
                )
            }
        }

    @Test
    fun `a message the log accepts is appended untouched`(al: BufferAllocator) = runTest {
        MemoryStorage(al, 0).use { bufferPool ->
            val declining = DecliningDriver(maxBytes = 1024)
            val noOp = ReplicaMessage.NoOp(srcMsgId = 4, termId = 1)

            offloading(bufferPool, declining, blockIndex = 7).appendToReplica(noOp)

            assertEquals(listOf<ReplicaMessage>(noOp), declining.appended)
            assertEquals(
                emptyList<Any>(),
                bufferPool.listAllObjects(ReplicaMessage.OversizedMessage.oversizedDir).toList(),
                "nothing reaches the object store",
            )
        }
    }

    @Test
    fun `a partition with no buffer pool reports the size failure rather than swallowing it`(al: BufferAllocator) =
        runTest {
            MemoryStorage(al, 0).use { bufferPool ->
                val driver = OffloadingLogsDriver(
                    DecliningDriver(maxBytes = 1024),
                    PartitionStorage(DatabaseLogs(null, null), null, null),
                    PartitionState(TableCatalog(bufferPool), null, null),
                )

                assertThrows<Log.MessageTooLargeException> { driver.appendToReplica(bigTriesAdded()) }
            }
        }
}
