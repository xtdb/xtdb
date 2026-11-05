package xtdb.trie

import com.carrotsearch.hppc.ByteArrayList
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.Schema
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import xtdb.api.TableRef
import xtdb.arrow.ByteArrayChannel
import xtdb.arrow.FixedSizeBinaryVector
import xtdb.arrow.Relation
import xtdb.arrow.RunEndEncodedVector
import xtdb.arrow.RunEndWidth
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorType.Companion.INSTANT
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.compactor.PageTree
import xtdb.storage.MemoryStorage
import xtdb.table.TableSlug
import xtdb.trie.PageTrieWriter.Companion.iidPartitions
import xtdb.trie.PageTrieWriter.Companion.partitionSlices
import xtdb.arrow.Encoding
import java.nio.ByteBuffer
import java.util.*

internal fun UUID.toBytes(): ByteArray =
    ByteBuffer.allocate(16).run {
        putLong(mostSignificantBits)
        putLong(leastSignificantBits)
    }.array()

internal fun <V> usingIidReader(allocator: BufferAllocator, iids: List<UUID>, f: (VectorReader) -> V): V =
    FixedSizeBinaryVector(allocator, "_iid", false, 16).use { vec ->
        iids.forEach { vec.writeBytes(ByteBuffer.wrap(it.toBytes())) }

        vec.valueCount = iids.size

        f(vec)
    }

internal class PageTrieWriterTest {

    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    @Test
    fun `test slicing selection`() {
        val selection = IntArray(10) { idx -> idx }
        assertArrayEquals(
            arrayOf(intArrayOf(0, 1, 2, 3), null, intArrayOf(4, 5, 6, 7, 8, 9), null),
            selection.partitionSlices(intArrayOf(0, 4, 4, 10))
        )
    }

    @Test
    fun `test partitioning selection`() {
        usingIidReader(allocator, emptyList()) { iidReader ->
            assertArrayEquals(
                arrayOfNulls(4),
                IntArray(0) { it }.iidPartitions(iidReader, 0),
                "empty selection"
            )
        }

        /*
         split to two levels, this looks like:
         `[[[0] nil nil [3]], [[4] [5] nil [7 7]], nil, [nil [d d] [e] nil]]`
         or, in indices
         `[[[0] nil nil [1]], [[2] [3] nil [4 5]], nil, [nil [6 7] [8] nil]]`
         */
        val iids = listOf(
            UUID.fromString("05000000-0000-0000-0000-000000000000"),
            UUID.fromString("35000000-0000-0000-0000-000000000000"),
            UUID.fromString("45000000-0000-0000-0000-000000000000"),
            UUID.fromString("55000000-0000-0000-0000-000000000000"),
            UUID.fromString("75000000-0000-0000-0000-000000000000"),
            UUID.fromString("75000000-0000-0000-0000-000000000000"),
            UUID.fromString("d5000000-0000-0000-0000-000000000000"),
            UUID.fromString("d5000000-0000-0000-0000-000000000000"),
            UUID.fromString("e5000000-0000-0000-0000-000000000000"),
        )

        usingIidReader(allocator, iids) { iidReader ->
            val l0Selection = IntArray(9) { it }.iidPartitions(iidReader, 0)

            assertArrayEquals(
                arrayOf(intArrayOf(0, 1), intArrayOf(2, 3, 4, 5), null, intArrayOf(6, 7, 8)),
                l0Selection,
                "top level"
            )

            assertArrayEquals(
                arrayOf(intArrayOf(2), intArrayOf(3), null, intArrayOf(4, 5)),
                l0Selection[1]!!.iidPartitions(iidReader, 1),
                "second level, idx 1"
            )

            assertArrayEquals(
                arrayOf(null, intArrayOf(6, 7), intArrayOf(8), null),
                l0Selection[3]!!.iidPartitions(iidReader, 1),
                "second level, idx 3"
            )

            assertArrayEquals(
                arrayOf(null, IntArray(9) { it }, null, null),
                IntArray(9) { it }.iidPartitions(iidReader, 3),
                "nothing in first or last partitions"
            )
        }
    }

    private data class Row(val iid: UUID, val systemFrom: Long, val recency: Long)

    private sealed interface Shape {
        data class Iid(val children: List<Shape?>) : Shape
        data class Recency(val buckets: List<Pair<RecencyMicros, Shape>>) : Shape
        data class Page(val rows: List<Row>, val iidRuns: Int?) : Shape
    }

    private val Shape.pages: List<Shape.Page>
        get() = when (this) {
            is Shape.Iid -> children.flatMap { it?.pages.orEmpty() }
            is Shape.Recency -> buckets.flatMap { it.second.pages }
            is Shape.Page -> listOf(this)
        }

    private val Shape.recencyBranches: List<Shape.Recency>
        get() = when (this) {
            is Shape.Iid -> children.flatMap { it?.recencyBranches.orEmpty() }
            is Shape.Recency -> listOf(this) + buckets.flatMap { it.second.recencyBranches }
            is Shape.Page -> emptyList()
        }

    private val slug = TableSlug.of(TableRef("public", "docs"))
    private val weekStart = 1_578_268_800_000_000L
    private val second = 1_000_000L
    private val hour = 3600 * second
    private val day = 24 * hour
    private val iidRuns = Encoding.RunEnd(RunEndWidth.I32)

    private fun ByteArray.toUuid() = ByteBuffer.wrap(this).let { UUID(it.getLong(), it.getLong()) }

    private fun iid(firstByte: Int, rnd: Random) =
        ByteArray(16).also { rnd.nextBytes(it); it[0] = firstByte.toByte() }.toUuid()

    private val iidOrder = Comparator<UUID> { a, b -> Arrays.compareUnsigned(a.toBytes(), b.toBytes()) }

    private fun writeTrie(
        bp: MemoryStorage, trieKey: String, rows: List<Row>,
        withRecency: Boolean, part: ByteArray?, pageSize: Int,
        iidEncoding: Encoding = Encoding.Plain, inputPageSize: Int = 50
    ) {
        val sorted = rows.sortedWith(compareBy(iidOrder) { row: Row -> row.iid }.thenByDescending { it.systemFrom })
        val plainSchema = Trie.dataRelSchema(null)
        val schema = Trie.dataRelSchema(null, iidEncoding)
            .let { if (withRecency) Schema(it.fields + (Trie.RECENCY_COL ofType INSTANT)) else it }

        val ch = ByteArrayChannel()
        val leaves = mutableListOf<PageTree.Leaf>()

        Relation(allocator, if (withRecency) Schema(plainSchema.fields + (Trie.RECENCY_COL ofType INSTANT)) else plainSchema).use { plainRel ->
            Relation(allocator, schema).use { outRel ->
                val copier = plainRel.rowCopierWithReeIid(outRel)

                outRel.startUnload(ch).use { unloader ->
                    for (page in sorted.chunked(inputPageSize)) {
                        for (row in page) {
                            plainRel["_iid"].writeBytes(row.iid.toBytes())
                            plainRel["_system_from"].writeLong(row.systemFrom)
                            plainRel["_valid_from"].writeLong(row.systemFrom)
                            plainRel["_valid_to"].writeLong(Long.MAX_VALUE)
                            plainRel["op"]["delete"].writeNull()
                            if (withRecency) plainRel[Trie.RECENCY_COL].writeLong(row.recency)
                            plainRel.endRow()
                        }

                        copier.copyRange(0, plainRel.rowCount)
                        unloader.writePage()

                        val recency =
                            if (withRecency) RecencyRange(page.minOf { it.recency }, page.maxOf { it.recency })
                            else RecencyRange.EMPTY
                        leaves += PageTree.Leaf(leaves.size, ByteArrayList(), page.size, recency)

                        plainRel.clear()
                        outRel.clear()
                    }

                    unloader.end()
                }
            }
        }

        Relation.loader(allocator, ch.toByteArray()).use { loader ->
            runBlocking {
                PageTrieWriter(allocator, bp, calculateBlooms = false)
                    .writePageTree(slug, trieKey, loader, leaves, pageSize, part)
            }
        }
    }

    private fun readTrie(bp: MemoryStorage, trieKey: String): Shape? =
        Relation.loader(allocator, bp.getByteArray(slug.metaFilePath(trieKey))).use { metaLoader ->
            Relation(allocator, metaLoader.schema).use { metaRel ->
                metaLoader.loadNextPage(metaRel)

                Relation.loader(allocator, bp.getByteArray(slug.dataFilePath(trieKey))).use { dataLoader ->
                    Relation(allocator, dataLoader.schema).use { dataRel ->
                        val ptr = ArrowBufPointer()

                        fun page(): Shape.Page {
                            val iids = dataRel["_iid"]
                            val recencies = dataRel.vectorForOrNull(Trie.RECENCY_COL)

                            val rows = List(dataRel.rowCount) { idx ->
                                val iid = iids.getPointer(idx, ptr).let { p -> ByteArray(16) { p.buf!!.getByte(p.offset + it) } }
                                Row(iid.toUuid(), dataRel["_system_from"].getLong(idx), recencies?.getLong(idx) ?: 0)
                            }

                            return Shape.Page(rows, (iids as? RunEndEncodedVector)?.runCount)
                        }

                        fun walk(node: HashTrie.Node<ArrowHashTrie.Leaf>?): Shape? =
                            when (node) {
                                null -> null
                                is HashTrie.IidBranch -> Shape.Iid(node.hashChildren.map { walk(it) })
                                is HashTrie.RecencyBranch -> Shape.Recency(node.buckets.map { it.maxRecency to walk(it.node)!! })
                                is HashTrie.Leaf -> {
                                    dataLoader.loadPage((node as ArrowHashTrie.Leaf).dataPageIndex, dataRel)
                                    page()
                                }
                            }

                        walk(ArrowHashTrie(metaRel["nodes"]).rootNode)
                    }
                }
            }
        }

    private fun Shape.withoutRecency(): Shape =
        when (this) {
            is Shape.Iid -> Shape.Iid(children.map { it?.withoutRecency() })
            is Shape.Recency -> Shape.Recency(buckets.map { (key, child) -> key to child.withoutRecency() })
            is Shape.Page -> Shape.Page(rows.map { it.copy(recency = 0) }, iidRuns)
        }

    private fun assertHoldsEachRowOnce(rows: List<Row>, trie: Shape, pageSize: Int) {
        assertEquals(rows.sortedWith(rowOrder), trie.pages.flatMap { it.rows }.sortedWith(rowOrder), "every row, once")
        assertTrue(trie.pages.all { it.rows.size <= pageSize }, "no page holds more than a page of rows")
    }

    private val rowOrder = compareBy(iidOrder) { row: Row -> row.iid }.thenBy { it.systemFrom }

    @Test
    fun `a file over several cells branches on recency at its part, each cell's pages holding only its own rows`() {
        val rnd = Random(0)
        val part = byteArrayOf(2)

        val rows = List(400) { n ->
            Row(iid(0x80 or rnd.nextInt(0x40), rnd), n.toLong(), weekStart + 1 + rnd.nextLong(12 * hour))
        }

        MemoryStorage(allocator, epoch = 0).use { bp ->
            writeTrie(bp, "l01-r20200113-p2-b00", rows, withRecency = true, part = part, pageSize = 8)

            val width = RecencyCells.width(rows.size.toLong(), RecencyRange(rows.minOf { it.recency }, rows.maxOf { it.recency }), 8)
            assertNotNull(width)

            val trie = readTrie(bp, "l01-r20200113-p2-b00")!!
            assertHoldsEachRowOnce(rows, trie, 8)

            val root = trie as Shape.Iid
            assertEquals(listOf(null, null, null), root.children.filterIndexed { idx, _ -> idx != 2 }, "the part pins the top level")

            val cells = root.children[2] as Shape.Recency
            assertEquals(cells.buckets.map { it.first }.sorted().distinct(), cells.buckets.map { it.first }, "cells ascend")

            for ((boundary, cell) in cells.buckets) {
                assertEquals(0, boundary % width!!, "a cell is keyed by its boundary")
                assertTrue(cell.pages.flatMap { it.rows }.all { RecencyCells.boundary(it.recency, width) == boundary })
            }
        }
    }

    @Test
    fun `without cells, a file's trie is the IID trie the plain writer builds from the same rows`() {
        val rnd = Random(1)

        val rows = List(60) { iid(rnd.nextInt(0x100), rnd) }
            .flatMap { iid -> List(1 + rnd.nextInt(3)) { v -> Row(iid, v.toLong(), weekStart + 1 + rnd.nextLong(7 * day - 1)) } }

        MemoryStorage(allocator, epoch = 0).use { bp ->
            writeTrie(bp, "l01-r20200113-b00", rows, withRecency = true, part = null, pageSize = 8)
            writeTrie(bp, "l01-r20200113-b01", rows, withRecency = false, part = null, pageSize = 8, inputPageSize = rows.size)

            val streamed = readTrie(bp, "l01-r20200113-b00")!!
            assertTrue(streamed.recencyBranches.isEmpty(), "no rung's cells average four pages")
            assertEquals(readTrie(bp, "l01-r20200113-b01"), streamed.withoutRecency())
        }
    }

    @ParameterizedTest(name = "REE iids: {0}")
    @ValueSource(booleans = [false, true])
    fun `more than a page of one entity streams out a page at a time, keyed by each page's newest row`(reeIids: Boolean) {
        val rnd = Random(2)

        fun neighbour(base: UUID, delta: Long) = UUID(base.mostSignificantBits, base.leastSignificantBits + delta)

        val e = iid(0x40, rnd)
        val g = iid(0xf0, rnd)

        fun versions(iid: UUID, n: Int) = List(n) { v -> Row(iid, v.toLong(), weekStart + 1 + (rnd.nextLong(50) * second)) }

        val rows = List(40) { iid(rnd.nextInt(0x100), rnd) }.flatMap { versions(it, 1) } +
            versions(neighbour(e, -1), 3) + versions(e, 30) + versions(neighbour(e, 1), 2) + versions(neighbour(e, 2), 20) +
            versions(g, 16)

        MemoryStorage(allocator, epoch = 0).use { bp ->
            writeTrie(
                bp, "l01-r20200113-b00", rows, withRecency = true, part = null, pageSize = 8,
                iidEncoding = if (reeIids) iidRuns else Encoding.Plain
            )

            val trie = readTrie(bp, "l01-r20200113-b00")!!
            assertHoldsEachRowOnce(rows, trie, 8)

            val entityPages = trie.recencyBranches
                .filter { branch -> branch.buckets.all { it.second is Shape.Page } }
                .associate { branch ->
                    val iids = branch.buckets.flatMap { (it.second as Shape.Page).rows }.map { it.iid }.toSet()
                    iids.single() to branch.buckets.map { (key, page) -> key to (page as Shape.Page).rows }
                }

            assertEquals(setOf(e, neighbour(e, 2), g), entityPages.keys)

            for ((iid, sizes) in listOf(e to listOf(8, 8, 8, 6), neighbour(e, 2) to listOf(8, 8, 4), g to listOf(8, 8))) {
                val pages = entityPages.getValue(iid)
                assertEquals(sizes.sorted(), pages.map { it.second.size }.sorted(), "page sizes")
                assertEquals(pages.map { (_, rows) -> rows.maxOf { it.recency } }, pages.map { it.first }, "keyed by the newest row")
                assertEquals(pages.map { it.first }.sorted(), pages.map { it.first }, "keys ascend")
            }

            if (reeIids)
                assertTrue(trie.pages.all { page -> page.iidRuns == page.rows.map { it.iid }.distinct().size }, "one run per entity per page")
        }
    }

    @Test
    fun `a file of one entity's versions is a recency branch of its pages`() {
        val rnd = Random(3)
        val e = iid(0x40, rnd)
        val rows = List(17) { v -> Row(e, v.toLong(), weekStart + hour) }

        MemoryStorage(allocator, epoch = 0).use { bp ->
            writeTrie(bp, "l01-r20200113-b00", rows, withRecency = true, part = null, pageSize = 8)

            val trie = readTrie(bp, "l01-r20200113-b00")!!
            assertHoldsEachRowOnce(rows, trie, 8)
            assertEquals(listOf(8, 8, 1), (trie as Shape.Recency).buckets.map { (it.second as Shape.Page).rows.size })
        }
    }
}
