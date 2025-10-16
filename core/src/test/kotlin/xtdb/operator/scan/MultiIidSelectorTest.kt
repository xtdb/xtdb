package xtdb.operator.scan

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.Bytes
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorWriter
import xtdb.trie.Bucketer
import xtdb.types.Type.Companion.IID
import java.nio.ByteBuffer
import java.util.*
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

@OptIn(ExperimentalUuidApi::class)
class MultiIidSelectorTest {
    private lateinit var al: BufferAllocator
    private val bucketer = Bucketer.DEFAULT

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        al.close()
    }

    private fun VectorWriter.writeUuid(uuid: Uuid) {
        writeBytes(ByteBuffer.wrap(uuid.toByteArray()))
    }

    private fun createIidSet(vararg uuids: Uuid): SortedSet<ByteArray> {
        return TreeSet(Bytes.COMPARATOR).apply {
            for (uuid in uuids) {
                add(uuid.toByteArray())
            }
        }
    }

    // Test the basic SelectionSpec.select method (without path filtering)
    @Test
    fun testBasicSelectWithEmptyIids() {
        val iidSet = createIidSet()
        val selector = MultiIidSelector(iidSet)

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID.fieldType)

            iids.writeUuid(Uuid.parse("80000000-0000-0000-0000-000000000000"))
            rel.endRow()
            iids.writeUuid(Uuid.parse("90000000-0000-0000-0000-000000000000"))
            rel.endRow()

            val result = selector.select(al, rel, emptyMap(), RelationReader.from(emptyList(), 0))
            assertArrayEquals(IntArray(0), result, "empty iid set should match nothing")
        }
    }

    @Test
    fun testBasicSelectWithEmptyRelation() {
        val iidSet = createIidSet(
            Uuid.parse("80000000-0000-0000-0000-000000000000")
        )
        val selector = MultiIidSelector(iidSet)

        Relation(al).use { rel ->
            rel.vectorFor("_iid", IID.fieldType)
            val result = selector.select(al, rel, emptyMap(), RelationReader.from(emptyList(), 0))
            assertArrayEquals(IntArray(0), result, "empty relation should match nothing")
        }
    }

    @Test
    fun testBasicSelectSingleMatch() {
        val searchUuid = Uuid.parse("80000000-0000-0000-0000-000000000000")
        val iidSet = createIidSet(searchUuid)
        val selector = MultiIidSelector(iidSet)

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID.fieldType)

            iids.writeUuid(Uuid.parse("70000000-0000-0000-0000-000000000000"))
            rel.endRow()
            iids.writeUuid(searchUuid)
            rel.endRow()
            iids.writeUuid(Uuid.parse("90000000-0000-0000-0000-000000000000"))
            rel.endRow()

            val result = selector.select(al, rel, emptyMap(), RelationReader.from(emptyList(), 0))
            assertArrayEquals(intArrayOf(1), result, "should match only the search UUID")
        }
    }

    @Test
    fun testBasicSelectMultipleMatches() {
        val uuid1 = Uuid.parse("20000000-0000-0000-0000-000000000000")
        val uuid2 = Uuid.parse("80000000-0000-0000-0000-000000000000")
        val uuid3 = Uuid.parse("f0000000-0000-0000-0000-000000000000")

        val iidSet = createIidSet(uuid1, uuid2, uuid3)
        val selector = MultiIidSelector(iidSet)

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID.fieldType)

            iids.writeUuid(Uuid.parse("10000000-0000-0000-0000-000000000000"))
            rel.endRow()
            iids.writeUuid(uuid1)
            rel.endRow()
            iids.writeUuid(Uuid.parse("50000000-0000-0000-0000-000000000000"))
            rel.endRow()
            iids.writeUuid(uuid2)
            rel.endRow()
            iids.writeUuid(uuid3)
            rel.endRow()

            val result = selector.select(al, rel, emptyMap(), RelationReader.from(emptyList(), 0))
            assertArrayEquals(intArrayOf(1, 3, 4), result, "should match all three search UUIDs")
        }
    }

    @Test
    fun testBasicSelectDuplicateIidsInRelation() {
        val searchUuid = Uuid.parse("80000000-0000-0000-0000-000000000000")
        val iidSet = createIidSet(searchUuid)
        val selector = MultiIidSelector(iidSet)

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID.fieldType)

            iids.writeUuid(searchUuid)
            rel.endRow()
            iids.writeUuid(searchUuid)
            rel.endRow()
            iids.writeUuid(searchUuid)
            rel.endRow()

            val result = selector.select(al, rel, emptyMap(), RelationReader.from(emptyList(), 0))
            assertArrayEquals(intArrayOf(0, 1, 2), result, "should match all duplicate occurrences")
        }
    }

    @Test
    fun testBasicSelectNoMatches() {
        val iidSet = createIidSet(
            Uuid.parse("80000000-0000-0000-0000-000000000000"),
            Uuid.parse("90000000-0000-0000-0000-000000000000")
        )
        val selector = MultiIidSelector(iidSet)

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID.fieldType)

            iids.writeUuid(Uuid.parse("10000000-0000-0000-0000-000000000000"))
            rel.endRow()
            iids.writeUuid(Uuid.parse("20000000-0000-0000-0000-000000000000"))
            rel.endRow()
            iids.writeUuid(Uuid.parse("30000000-0000-0000-0000-000000000000"))
            rel.endRow()

            val result = selector.select(al, rel, emptyMap(), RelationReader.from(emptyList(), 0))
            assertArrayEquals(IntArray(0), result, "should match nothing when all IIDs are different")
        }
    }

    // Test the path-based select method with binary search optimization
    @Test
    fun testPathBasedSelectEmpty() {
        val iidSet = createIidSet()
        val selector = MultiIidSelector(iidSet)
        val path = ByteArray(0)

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID.fieldType)

            iids.writeUuid(Uuid.parse("80000000-0000-0000-0000-000000000000"))
            rel.endRow()

            val result = selector.select(al, rel, path)
            assertArrayEquals(IntArray(0), result, "empty iid set should match nothing")
        }
    }

    @Test
    fun testPathBasedSelectSingleMatch() {
        val uuid = Uuid.parse("80000000-0000-0000-0000-000000000000")
        val iidSet = createIidSet(uuid)
        val selector = MultiIidSelector(iidSet)
        val path = ByteArray(0)

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID.fieldType)

            iids.writeUuid(Uuid.parse("70000000-0000-0000-0000-000000000000"))
            rel.endRow()
            iids.writeUuid(uuid)
            rel.endRow()
            iids.writeUuid(Uuid.parse("90000000-0000-0000-0000-000000000000"))
            rel.endRow()

            val result = selector.select(al, rel, path)
            assertArrayEquals(intArrayOf(1), result, "should find the matching UUID")
        }
    }

    @Test
    fun testPathBasedSelectMultipleMatches() {
        val uuid1 = Uuid.parse("20000000-0000-0000-0000-000000000000")
        val uuid2 = Uuid.parse("80000000-0000-0000-0000-000000000000")
        val uuid3 = Uuid.parse("f0000000-0000-0000-0000-000000000000")

        val iidSet = createIidSet(uuid1, uuid2, uuid3)
        val selector = MultiIidSelector(iidSet)
        val path = ByteArray(0)

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID.fieldType)

            iids.writeUuid(Uuid.parse("10000000-0000-0000-0000-000000000000"))
            rel.endRow()
            iids.writeUuid(uuid1)
            rel.endRow()
            iids.writeUuid(Uuid.parse("50000000-0000-0000-0000-000000000000"))
            rel.endRow()
            iids.writeUuid(uuid2)
            rel.endRow()
            iids.writeUuid(uuid3)
            rel.endRow()

            val result = selector.select(al, rel, path)
            assertArrayEquals(intArrayOf(1, 3, 4), result, "should match all search UUIDs")
        }
    }

    @Test
    fun testPathBasedSelectWithPathFiltering() {
        // Create IIDs that will fall into different buckets based on path
        // With DEFAULT_LEVEL_BITS = 2, we have 4 buckets (0x00, 0x40, 0x80, 0xC0)
        val uuid1 = Uuid.parse("00000000-0000-0000-0000-000000000000") // bucket 0
        val uuid2 = Uuid.parse("40000000-0000-0000-0000-000000000000") // bucket 1
        val uuid3 = Uuid.parse("80000000-0000-0000-0000-000000000000") // bucket 2
        val uuid4 = Uuid.parse("c0000000-0000-0000-0000-000000000000") // bucket 3

        val iidSet = createIidSet(uuid1, uuid2, uuid3, uuid4)
        val selector = MultiIidSelector(iidSet)

        // Path bytearray(0) filters to bucket 0 (0x00 prefix)
        val path = byteArrayOf(0)

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID.fieldType)

            iids.writeUuid(uuid1)
            rel.endRow()
            iids.writeUuid(uuid2)
            rel.endRow()
            iids.writeUuid(uuid3)
            rel.endRow()
            iids.writeUuid(uuid4)
            rel.endRow()

            val result = selector.select(al, rel, path)
            // Should only match UUIDs that fall within the path's bucket range
            assertArrayEquals(intArrayOf(0), result, "should only match IIDs in the specified path bucket")
        }
    }

    @Test
    fun testPathBasedSelectDuplicatesInRelation() {
        val uuid = Uuid.parse("80000000-0000-0000-0000-000000000000")
        val iidSet = createIidSet(uuid)
        val selector = MultiIidSelector(iidSet)
        val path = ByteArray(0)

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID.fieldType)

            iids.writeUuid(uuid)
            rel.endRow()
            iids.writeUuid(uuid)
            rel.endRow()
            iids.writeUuid(uuid)
            rel.endRow()

            val result = selector.select(al, rel, path)
            assertArrayEquals(intArrayOf(0, 1, 2), result, "should match all duplicates")
        }
    }

    @Test
    fun testPathBasedSelectLargeDataset() {
        // Test with larger dataset - 10% hit rate (100 matches in 1000 rows)
        // Create 100 search UUIDs evenly distributed: every 10th position
        val searchUuids = (0..99).map { i ->
            Uuid.parse("${String.format("%08x", i * 10)}-0000-0000-0000-000000000000")
        }
        val iidSet = createIidSet(*searchUuids.toTypedArray())
        val selector = MultiIidSelector(iidSet)
        val path = ByteArray(0)

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID.fieldType)

            // Create 1000 sorted rows where every 10th row matches
            // Row i gets UUID with value i in hex, ensuring natural sort order
            for (i in 0 until 1000) {
                val uuid = Uuid.parse("${String.format("%08x", i)}-0000-0000-0000-000000000000")
                iids.writeUuid(uuid)
                rel.endRow()
            }

            val result = selector.select(al, rel, path)
            // Should find all 100 matches at positions 0, 10, 20, ..., 990
            val expectedIndices = (0..99).map { it * 10 }.toIntArray()
            assertArrayEquals(expectedIndices, result, "should find all 100 matches at every 10th position")
        }
    }

    @Test
    fun testPathBasedSelectSortedOrder() {
        // Ensure results maintain sorted order
        val uuid1 = Uuid.parse("20000000-0000-0000-0000-000000000000")
        val uuid2 = Uuid.parse("50000000-0000-0000-0000-000000000000")
        val uuid3 = Uuid.parse("80000000-0000-0000-0000-000000000000")

        val iidSet = createIidSet(uuid3, uuid1, uuid2) // Insert out of order
        val selector = MultiIidSelector(iidSet)
        val path = ByteArray(0)

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID.fieldType)

            iids.writeUuid(Uuid.parse("10000000-0000-0000-0000-000000000000"))
            rel.endRow()
            iids.writeUuid(uuid1)
            rel.endRow()
            iids.writeUuid(Uuid.parse("40000000-0000-0000-0000-000000000000"))
            rel.endRow()
            iids.writeUuid(uuid2)
            rel.endRow()
            iids.writeUuid(Uuid.parse("70000000-0000-0000-0000-000000000000"))
            rel.endRow()
            iids.writeUuid(uuid3)
            rel.endRow()

            val result = selector.select(al, rel, path)
            assertArrayEquals(intArrayOf(1, 3, 5), result, "results should be in sorted row order")
        }
    }

    @Test
    fun testPathBasedSelectBinarySearchTrigger() {
        // Test case designed to trigger binary search optimization
        // Large haystack with sparse needles
        val searchUuids = listOf(
            Uuid.parse("00000064-0000-0000-0000-000000000000"),  // 100 in hex
            Uuid.parse("000001f4-0000-0000-0000-000000000000"),  // 500 in hex
            Uuid.parse("00000384-0000-0000-0000-000000000000")   // 900 in hex
        )
        val iidSet = createIidSet(*searchUuids.toTypedArray())
        val selector = MultiIidSelector(iidSet)
        val path = ByteArray(0)

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID.fieldType)

            // Create 1000 rows where the UUID matches the row index in hex
            // This ensures natural sorting and makes positions predictable
            for (i in 0 until 1000) {
                val uuid = Uuid.parse("${String.format("%08x", i)}-0000-0000-0000-000000000000")
                iids.writeUuid(uuid)
                rel.endRow()
            }

            val result = selector.select(al, rel, path)
            assertArrayEquals(intArrayOf(100, 500, 900), result, "should find sparse matches efficiently")
        }
    }

    @Test
    fun testPathBasedSelectNoMatches() {
        val iidSet = createIidSet(
            Uuid.parse("80000000-0000-0000-0000-000000000000"),
            Uuid.parse("90000000-0000-0000-0000-000000000000")
        )
        val selector = MultiIidSelector(iidSet)
        val path = ByteArray(0)

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID.fieldType)

            iids.writeUuid(Uuid.parse("10000000-0000-0000-0000-000000000000"))
            rel.endRow()
            iids.writeUuid(Uuid.parse("20000000-0000-0000-0000-000000000000"))
            rel.endRow()

            val result = selector.select(al, rel, path)
            assertArrayEquals(IntArray(0), result, "should match nothing")
        }
    }

    @Test
    fun testPathBasedSelectAllNeedlesBeforeHaystack() {
        // All search IIDs are lexicographically before relation IIDs
        val iidSet = createIidSet(
            Uuid.parse("10000000-0000-0000-0000-000000000000"),
            Uuid.parse("20000000-0000-0000-0000-000000000000")
        )
        val selector = MultiIidSelector(iidSet)
        val path = ByteArray(0)

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID.fieldType)

            iids.writeUuid(Uuid.parse("80000000-0000-0000-0000-000000000000"))
            rel.endRow()
            iids.writeUuid(Uuid.parse("90000000-0000-0000-0000-000000000000"))
            rel.endRow()

            val result = selector.select(al, rel, path)
            assertArrayEquals(IntArray(0), result, "should find no matches when needles before haystack")
        }
    }

    @Test
    fun testPathBasedSelectAllNeedlesAfterHaystack() {
        // All search IIDs are lexicographically after relation IIDs
        val iidSet = createIidSet(
            Uuid.parse("f0000000-0000-0000-0000-000000000000"),
            Uuid.parse("f1000000-0000-0000-0000-000000000000")
        )
        val selector = MultiIidSelector(iidSet)
        val path = ByteArray(0)

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID.fieldType)

            iids.writeUuid(Uuid.parse("10000000-0000-0000-0000-000000000000"))
            rel.endRow()
            iids.writeUuid(Uuid.parse("20000000-0000-0000-0000-000000000000"))
            rel.endRow()

            val result = selector.select(al, rel, path)
            assertArrayEquals(IntArray(0), result, "should find no matches when needles after haystack")
        }
    }
}
