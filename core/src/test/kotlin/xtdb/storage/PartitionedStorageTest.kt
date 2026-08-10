package xtdb.storage

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.util.StringUtil.asLexHex
import xtdb.util.asPath

/**
 * Partition assertions that only mean anything when [openPartitionedStorage] hands back pools over a
 * shared underlying store, so the isolation below is real path-scoping. MemoryStorage gets a fresh
 * store per open and so deliberately doesn't extend this — the assertions would hold even if the
 * partition index were ignored entirely.
 */
abstract class PartitionedStorageTest : StorageTest() {

    @Test
    fun partitionedPoolsAreIsolated() {
        openPartitionedStorage(0, 2).use { p0 ->
            openPartitionedStorage(1, 2).use { p1 ->
                // the same key in both partitions — sizes distinguish who wrote what
                p0.writeBlock(0, size = 3)
                p1.writeBlock(0, size = 7)
                p1.writeBlock(1, size = 7)

                assertEquals(
                    listOf(StoredObject("blocks/b${0L.asLexHex}.binpb".asPath, 3)),
                    p0.listAllObjects().toList(),
                    "partition 0 lists only its own objects"
                )
                assertEquals(
                    listOf(
                        StoredObject("blocks/b${0L.asLexHex}.binpb".asPath, 7),
                        StoredObject("blocks/b${1L.asLexHex}.binpb".asPath, 7),
                    ),
                    p1.listAllObjects().toList(),
                    "partition 1 lists only its own objects"
                )

                assertEquals(0L, p0.latestAvailableBlockIndex(), "block discovery is per-partition")
                assertEquals(1L, p1.latestAvailableBlockIndex(), "block discovery is per-partition")

                // delete is GC's primitive — a partition's GC must never touch a sibling's files
                p0.deleteIfExistsSync("blocks/b${0L.asLexHex}.binpb".asPath)

                assertEquals(emptyList<StoredObject>(), p0.listAllObjects().toList())
                assertEquals(2, p1.listAllObjects().count(), "partition 1 unaffected by partition 0's delete")
            }
        }
    }
}
