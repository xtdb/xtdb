package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.io.TempDir
import xtdb.api.ICursor
import xtdb.arrow.Relation
import xtdb.arrow.VectorType.Companion.I64
import xtdb.operator.OrderSpec.Direction.ASC
import xtdb.operator.OrderSpec.NullOrdering.NULLS_LAST
import xtdb.test.AllocatorResolver
import java.nio.file.Files
import java.nio.file.Path
import java.time.ZoneOffset
import kotlin.io.path.listDirectoryEntries
import kotlin.random.Random

@ExtendWith(AllocatorResolver::class)
class ExternalSorterTest {

    @TempDir
    lateinit var spillRoot: Path

    private var spillDirsOpened = 0

    private fun sorter(al: BufferAllocator, rowLimit: Long = Long.MAX_VALUE) =
        ExternalSorter(al, mapOf("a" to I64), listOf(OrderSpec("a", ASC, NULLS_LAST)), ZoneOffset.UTC, rowLimit, blockSize = 10) {
            spillDirsOpened++
            Files.createTempDirectory(spillRoot, "external-sort")
        }

    private fun shuffled(n: Int) = (0L until n).shuffled(Random(6401))

    private fun ExternalSorter.appendPages(al: BufferAllocator, values: List<Long>, pageSize: Int) {
        for (page in values.chunked(pageSize)) {
            Relation.openFromCols(al, mapOf("a" to page)).use { append(it) }
        }
    }

    private fun ICursor.drain(): List<Long> = buildList {
        forEachRemaining { page ->
            val a = page["a"]
            for (idx in 0 until page.rowCount) add(a.getLong(idx))
        }
    }

    private fun spillFiles() = spillRoot.listDirectoryEntries().flatMap { it.listDirectoryEntries() }

    @Test
    fun `under the threshold, sorts in memory without opening a spill dir`(al: BufferAllocator) {
        sorter(al).use { sorter ->
            sorter.appendPages(al, shuffled(10), 3)
            assertEquals((0L until 10).toList(), sorter.finish().drain())
        }

        assertEquals(0, spillDirsOpened)
    }

    @Test
    fun `over the threshold, spills a run and still sorts`(al: BufferAllocator) {
        sorter(al).use { sorter ->
            sorter.appendPages(al, shuffled(25), 5)
            assertEquals(1, spillFiles().size, "one run spilled")
            assertEquals((0L until 25).toList(), sorter.finish().drain())
        }

        assertEquals(1, spillDirsOpened)
    }

    @Test
    fun `more runs than the merge fan-in merge over several passes`(al: BufferAllocator) {
        sorter(al).use { sorter ->
            sorter.appendPages(al, shuffled(110), 11)
            assertEquals(10, spillFiles().size, "a run per page")
            assertEquals((0L until 110).toList(), sorter.finish().drain())
        }
    }

    @Test
    fun `a row limit within half the threshold trims in memory rather than spilling`(al: BufferAllocator) {
        sorter(al, rowLimit = 3).use { sorter ->
            sorter.appendPages(al, shuffled(100), 7)
            assertEquals(listOf(0L, 1L, 2L), sorter.finish().drain())
        }

        assertEquals(0, spillDirsOpened)
    }

    @Test
    fun `a row limit over half the threshold spills, keeping only the first rows`(al: BufferAllocator) {
        sorter(al, rowLimit = 8).use { sorter ->
            sorter.appendPages(al, shuffled(100), 7)
            assertEquals((0L until 8).toList(), sorter.finish().drain())
        }

        assertEquals(1, spillDirsOpened)
    }

    @Test
    fun `the spill dir is deleted on close after a full drain`(al: BufferAllocator) {
        sorter(al).use { sorter ->
            sorter.appendPages(al, shuffled(50), 7)
            sorter.finish().drain()
            assertTrue(spillFiles().isNotEmpty(), "the merged run is still there")
        }

        assertEquals(emptyList<Path>(), spillRoot.listDirectoryEntries())
    }

    @Test
    fun `the spill dir is deleted on close after only part of the sorted pages are read`(al: BufferAllocator) {
        sorter(al).use { sorter ->
            sorter.appendPages(al, shuffled(50), 7)
            assertTrue(sorter.finish().tryAdvance { })
        }

        assertEquals(emptyList<Path>(), spillRoot.listDirectoryEntries())
    }

    @Test
    fun `the spill dir is deleted on close before finish`(al: BufferAllocator) {
        sorter(al).use { sorter ->
            sorter.appendPages(al, (0L until 25).toList(), 25)
            assertTrue(spillFiles().isNotEmpty(), "spilled a run")
        }

        assertEquals(emptyList<Path>(), spillRoot.listDirectoryEntries())
    }
}
