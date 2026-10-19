package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorUnloader
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver

@ExtendWith(AllocatorResolver::class)
class SharedEmptyBufferTest {

    // `allocator.empty` is one ArrowBuf for the whole JVM: moving its writer index stands in for another thread doing so.
    private fun <R> withSharedEmptyIndexMoved(al: BufferAllocator, f: () -> R): R {
        val shared = al.empty
        shared.writerIndex(777)
        try {
            return f()
        } finally {
            shared.readerIndex(0)
            shared.writerIndex(0)
        }
    }

    @Test
    fun `an empty buffer writes from its own index`(al: BufferAllocator) = withSharedEmptyIndexMoved(al) {
        ExtensibleBuffer(al).use { buf ->
            buf.writeInt(42)
            assertEquals(42, buf.getInt(0))
        }

        ExtensibleBuffer(al, 0).use { buf ->
            buf.writeInt(42)
            assertEquals(42, buf.getInt(0))
        }

        ExtensibleBuffer(al).use { src ->
            src.openSlice(al).use { slice ->
                slice.writeInt(42)
                assertEquals(42, slice.getInt(0))
            }
        }

        ExtensibleBuffer(al).use { buf ->
            buf.loadBuffer(al.empty)
            buf.writeInt(42)
            assertEquals(42, buf.getInt(0))
        }
    }

    @Test
    fun `record batches never carry the shared empty buffer`(al: BufferAllocator) = withSharedEmptyIndexMoved(al) {
        val rows = listOf(
            mapOf("i" to 1L, "s" to "a", "st" to mapOf("x" to 1L), "l" to listOf(1L, 2L), "u" to 1L, "b" to true),
            mapOf("i" to 2L, "s" to null, "st" to mapOf("x" to 2L, "y" to "z"), "l" to emptyList<Long>(), "u" to "str", "b" to false)
        )

        Relation.openFromRows(al, rows).use { rel ->
            Relation(al, rel.schema).use { empty ->
                for (r in listOf(rel, empty)) {
                    r.openArrowRecordBatch().use { rb -> assertTrue(rb.buffers.none { it === al.empty }) }
                    r.openDirectSlice(al).use { slice ->
                        slice.openArrowRecordBatch().use { rb -> assertTrue(rb.buffers.none { it === al.empty }) }
                    }
                }

                rel.openArrowRecordBatch().use { rb ->
                    Relation(al, rel.schema).use { loaded ->
                        loaded.load(rb)
                        assertEquals(rel.asMaps, loaded.asMaps)
                    }
                }

                rel.rowCopier(empty).copyRange(0, rel.rowCount)
                assertEquals(rel.asMaps, empty.asMaps)
            }
        }

        assertEquals(777, al.empty.writerIndex())
    }

    @Test
    fun `an empty list vector's batch survives Arrow Java's unloader`(al: BufferAllocator) {
        Relation.openFromRows(al, listOf(mapOf("l" to listOf(1L), "u" to listOf("a")), mapOf("u" to 1L))).use { rel ->
            Relation(al, rel.schema).use { empty ->
                for (r in listOf(empty, rel)) {
                    r.openAsRoot(al).use { root ->
                        VectorUnloader(root).recordBatch.use { rb -> rb.buffers.forEach { it.nioBuffer() } }
                    }
                }
            }
        }
    }
}
