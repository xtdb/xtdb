package xtdb.index

import com.carrotsearch.hppc.ByteArrayList
import xtdb.trie.BlockIndex
import xtdb.trie.FileSize
import xtdb.trie.Trie

private const val BRANCH_FACTOR = 4

/** One index compaction: [index]'s [inputs], replaced by the union of their entries, written at [outputs]. */
data class IndexJob(val index: IndexDef, val inputs: List<Trie.Key>, val outputs: List<Trie.Key>)

/**
 * Selects index compaction jobs from an index catalog: a pure function of it, so any node selects the same jobs from the same catalog.
 *
 * Each index's Live tries split in two by the index's first flushed block: the blocks below it, which backfill covers in any
 * order, and the rest, which block flushes cover in order.
 * Within each, the tree is the data tree's current side: L0s levelled into L1, then [BRANCH_FACTOR] full-size L1s, or
 * [BRANCH_FACTOR] tries at one level and part from L2 on, tiered into [BRANCH_FACTOR] tries a level and a hash bucket deeper.
 *
 * A backfill L0 is levelled only once no block below it is owed, so each side levels its L0s in block order.
 * That makes an output key name the same blocks whichever node, at whichever log position, selects it — so the files
 * two nodes write at one key hold the same entries.
 */
object IndexJobCalculator {

    fun availableJobs(cat: IndexCatalog, fileSizeTarget: FileSize): List<IndexJob> =
        cat.indices
            .filter { it.state !is IndexState.Dropped }
            .flatMap { index ->
                val (backfilled, flushed) = index.liveTries.partition { it.key.blockIndex < index.firstFlushedBlock }
                val owed = (index.state as? IndexState.Building)?.owedBlocks.orEmpty()

                index.jobs(backfilled, owed, fileSizeTarget) + index.jobs(flushed, emptySet(), fileSizeTarget)
            }

    private fun SecondaryIndex.jobs(tries: List<IndexTrie>, owed: Set<BlockIndex>, fileSizeTarget: FileSize): List<IndexJob> =
        listOfNotNull(levellingJob(tries, owed, fileSizeTarget)) + tieringJobs(tries, fileSizeTarget)

    private fun SecondaryIndex.levellingJob(tries: List<IndexTrie>, owed: Set<BlockIndex>, fileSizeTarget: FileSize): IndexJob? {
        val l0 = tries.filter { it.key.level == 0L }.minByOrNull { it.key.blockIndex } ?: return null
        if (owed.any { it < l0.key.blockIndex }) return null

        val l1 = tries.filter { it.key.level == 1L }.maxByOrNull { it.key.blockIndex }
            ?.takeIf { it.dataFileSize < fileSizeTarget }

        return IndexJob(def, listOfNotNull(l1, l0).map { it.key }, listOf(Trie.Key(1, null, null, l0.key.blockIndex)))
    }

    private fun SecondaryIndex.tieringJobs(tries: List<IndexTrie>, fileSizeTarget: FileSize): List<IndexJob> =
        tries
            .filter { it.key.level > 1 || (it.key.level == 1L && it.dataFileSize >= fileSizeTarget) }
            .groupBy { it.key.level to it.key.part?.toArray()?.toList().orEmpty() }
            .mapNotNull { (levelPart, levelTries) ->
                val (level, part) = levelPart
                val inputs = levelTries.map { it.key }.sortedBy { it.blockIndex }.take(BRANCH_FACTOR)
                if (inputs.size < BRANCH_FACTOR) return@mapNotNull null

                IndexJob(
                    def, inputs,
                    (0 until BRANCH_FACTOR).map { bucket ->
                        Trie.Key(level + 1, null, ByteArrayList.from(*(part + bucket.toByte()).toByteArray()), inputs.last().blockIndex)
                    },
                )
            }
}
