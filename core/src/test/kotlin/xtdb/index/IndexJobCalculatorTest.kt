package xtdb.index

import com.carrotsearch.hppc.ByteArrayList
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import xtdb.api.TableRef
import xtdb.trie.BlockIndex
import xtdb.trie.Trie
import java.time.Instant
import java.util.UUID
import kotlin.random.Random

class IndexJobCalculatorTest {
    private val def = IndexDef(UUID.fromString("00000000-0000-0000-0000-00000000000a"), "by_email", TableRef("public", "users"), "email")

    private fun key(level: Long, block: BlockIndex, vararg part: Int) =
        Trie.Key(level, null, if (part.isEmpty()) null else ByteArrayList.from(*part.map { it.toByte() }.toByteArray()), block)

    private fun catalog(firstFlushedBlock: BlockIndex, owed: Set<BlockIndex>, vararg tries: IndexTrie) =
        IndexCatalog.EMPTY.withIndex(def, firstFlushedBlock, owed)
            .let { cat -> tries.fold(cat) { c, trie -> c.withL0s(mapOf(def.id to trie)) } }

    private fun jobs(cat: IndexCatalog) = IndexJobCalculator.availableJobs(cat, 100).map { it.inputs to it.outputs }

    @Test
    fun `the oldest L0 levels into an undersized L1, and a full L1 takes no more`() {
        assertEquals(
            listOf(listOf(key(1, 1), key(0, 2)) to listOf(key(1, 2))),
            jobs(catalog(0, emptySet(), IndexTrie(key(0, 3), 10), IndexTrie(key(0, 2), 10), IndexTrie(key(1, 1), 99))),
        )

        assertEquals(
            listOf(listOf(key(0, 2)) to listOf(key(1, 2))),
            jobs(catalog(0, emptySet(), IndexTrie(key(0, 2), 10), IndexTrie(key(1, 1), 100))),
        )
    }

    @Test
    fun `four tries at one level and part tier into four, a level and a hash bucket deeper`() {
        assertEquals(
            listOf(listOf(key(1, 1), key(1, 2), key(1, 3), key(1, 4)) to (0..3).map { key(2, 4, it) }),
            jobs(catalog(0, emptySet(), *(5L downTo 1L).map { IndexTrie(key(1, it), 100) }.toTypedArray())),
            "the four oldest full L1s",
        )

        assertEquals(
            listOf(listOf(key(2, 4, 1), key(2, 8, 1), key(2, 12, 1), key(2, 16, 1)) to (0..3).map { key(3, 16, 1, it) }),
            jobs(
                catalog(
                    0, emptySet(),
                    *listOf(4L, 8L, 12L, 16L).map { IndexTrie(key(2, it, 1), 1) }.toTypedArray(),
                    *listOf(4L, 8L, 12L).map { IndexTrie(key(2, it, 2), 1) }.toTypedArray(),
                ),
            ),
            "L2 tries tier whatever their size, by part",
        )
    }

    @Test
    fun `a backfilled L0 waits until no block below it is owed, while flushed L0s level as they arrive`() {
        val l0s = arrayOf(IndexTrie(key(0, 2), 10), IndexTrie(key(0, 5), 10))

        assertEquals(listOf(listOf(key(0, 5)) to listOf(key(1, 5))), jobs(catalog(5, setOf(0, 1), *l0s)))
        assertEquals(
            listOf(listOf(key(0, 2)) to listOf(key(1, 2)), listOf(key(0, 5)) to listOf(key(1, 5))),
            jobs(catalog(5, setOf(3), *l0s)),
        )
    }

    @Test
    fun `tries either side of the first flushed block never share a job`() {
        assertEquals(
            listOf(listOf(key(0, 5)) to listOf(key(1, 5))),
            jobs(catalog(5, emptySet(), IndexTrie(key(1, 4), 10), IndexTrie(key(0, 5), 10))),
            "a backfilled L1 is no flushed L0's partner",
        )

        assertEquals(
            emptyList<Any>(),
            jobs(catalog(5, emptySet(), *listOf(3L, 4L, 5L, 6L).map { IndexTrie(key(1, it), 100) }.toTypedArray())),
            "two full L1s either side are not four",
        )
    }

    @Test
    fun `a dropped index compacts nothing`() {
        assertEquals(
            emptyList<Any>(),
            jobs(catalog(0, emptySet(), IndexTrie(key(0, 1), 10)).dropped(def.id, Instant.EPOCH)),
        )
    }

    // Each block's L0 holds a few (block, hash) entries; a trie's entries are the union of its inputs' within its part.
    private class Model(seed: Int) {
        val rnd = Random(seed)
        val blockCount = 40L
        val firstFlushedBlock = rnd.nextLong(0, 12)
        val def = IndexDef(UUID(0, seed.toLong()), "i", TableRef("public", "docs"), "v")

        val blockEntries: Map<BlockIndex, Set<Pair<BlockIndex, Long>>> =
            (0 until blockCount).associateWith { b ->
                if (rnd.nextInt(5) == 0) emptySet() else List(rnd.nextInt(1, 7)) { b to rnd.nextLong() }.toSet()
            }

        val entriesAt = mutableMapOf<Trie.Key, Set<Pair<BlockIndex, Long>>>()
        val added = mutableSetOf<Pair<BlockIndex, Long>>()

        var cat = IndexCatalog.EMPTY.withIndex(def, firstFlushedBlock, (0 until firstFlushedBlock).toSet())
        val toBackfill = (0 until firstFlushedBlock).shuffled(rnd).toMutableList()
        var nextFlush = firstFlushedBlock
        val planned = mutableListOf<Pair<IndexJob, List<IndexTrie>>>()
        val levelledBy = mutableMapOf<Trie.Key, IndexJob>()

        fun l0(block: BlockIndex): IndexTrie? =
            blockEntries.getValue(block).takeIf { it.isNotEmpty() }?.let { entries ->
                val key = Trie.l0Key(block)
                entriesAt[key] = entries
                added += entries
                IndexTrie(key, entries.size * 10L)
            }

        fun flush() {
            if (nextFlush >= blockCount) return
            l0(nextFlush++)?.let { cat = cat.withL0s(mapOf(def.id to it)) }
        }

        fun backfill() {
            val block = toBackfill.removeFirstOrNull() ?: return
            cat = cat.backfilled(def.id, block, l0(block))
            if ((cat[def.id]?.state as? IndexState.Building)?.complete == true) cat = cat.ready(def.id)
        }

        fun plan(job: IndexJob): List<IndexTrie> {
            val union = job.inputs.flatMap { entriesAt.getValue(it) }.toSet()
            return job.outputs.mapNotNull { out ->
                val entries = union.filter { (_, hash) -> out.partHolds(hash) }.toSet()
                entriesAt[out]?.let { assertEquals(it, entries, "two plans write different entries at $out") }
                entriesAt[out] = entries
                if (entries.isEmpty()) null else IndexTrie(out, entries.size * 10L)
            }
        }

        fun availableJobs(): List<IndexJob> {
            val jobs = IndexJobCalculator.availableJobs(cat, 100)

            for (job in jobs.filter { it.outputs.first().level == 1L })
                for (l0 in job.inputs.filter { it.level == 0L })
                    levelledBy.put(l0, job)?.let { assertEquals(it, job, "$l0 is an input to two L1-producing jobs") }

            return jobs
        }

        fun planJob() {
            val jobs = availableJobs()
            if (jobs.isNotEmpty()) jobs.random(rnd).let { planned += it to plan(it) }
        }

        val tieredUpTo = mutableMapOf<Triple<Boolean, Long, List<Byte>>, BlockIndex>()

        fun Trie.Key.levelPart() = Triple(blockIndex < firstFlushedBlock, level, part?.toArray()?.toList().orEmpty())

        fun publish(job: IndexJob, outputs: List<IndexTrie>, asOf: Instant) {
            val live = cat[def.id]!!.liveTries.map { it.key }
            cat = cat.compacted(def.id, outputs, job.inputs, asOf)

            if (job.outputs.first().level > 1 && live.containsAll(job.inputs))
                tieredUpTo.merge(job.inputs.first().levelPart(), job.inputs.maxOf { it.blockIndex }, ::maxOf)

            for (trie in cat[def.id]!!.liveTries)
                tieredUpTo[trie.key.levelPart()]?.let { upTo ->
                    assertTrue(trie.key.blockIndex > upTo, "${trie.key} is live below a tier that has taken its level up to block $upTo")
                }
        }

        fun liveEntries() = cat[def.id]!!.liveTries.flatMapTo(mutableSetOf()) { entriesAt.getValue(it.key) }
    }

    @Test
    fun `live tries always hold every entry ever added, and each level tiers in block order, across late backfills, stale and duplicate publishes`() {
        repeat(300) { seed ->
            val model = Model(seed)
            var now = Instant.EPOCH

            repeat(600) {
                now = now.plusSeconds(1)
                when (model.rnd.nextInt(6)) {
                    0 -> model.flush()
                    1 -> model.backfill()
                    2 -> model.planJob()
                    3, 4 -> model.planned.removeFirstOrNull()?.let { (job, outs) -> model.publish(job, outs, now) }
                    5 -> model.planned.randomOrNull(model.rnd)?.let { (job, outs) -> model.publish(job, outs, now) }
                }

                assertEquals(model.added, model.liveEntries(), "seed $seed")
            }

            while (true) {
                val job = model.availableJobs().firstOrNull() ?: break
                model.publish(job, model.plan(job), now)
                assertEquals(model.added, model.liveEntries(), "seed $seed")
            }
        }
    }
}
