@file:JvmName("Compactor")

package xtdb

import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.trie.HashTrie
import xtdb.trie.ITrieWriter
import xtdb.trie.LEVEL_WIDTH
import xtdb.vector.IVectorReader
import xtdb.vector.RelationReader

internal fun IntArray.partitionSlices(partIdxs: IntArray) =
    Array(LEVEL_WIDTH) { partition ->
        val cur = partIdxs[partition]
        val nxt = if (partition == partIdxs.lastIndex) size else partIdxs[partition + 1]

        if (cur == nxt) null else sliceArray(cur..<nxt)
    }

internal fun IntArray.partitionSelection(iidReader: IVectorReader, level: Int): Array<IntArray?> {
    val iidPtr = ArrowBufPointer()

    // for each partition, find the starting index in the selection
    val idxs = IntArray(LEVEL_WIDTH) { partition ->
        var left = 0
        var right = size
        var mid: Int
        while (left < right) {
            mid = (left + right) / 2

            val bucket = HashTrie.bucketFor(iidReader.getPointer(this[mid], iidPtr), level)

            if (bucket < partition) left = mid + 1 else right = mid
        }

        left
    }

    // slice the selection array for each partition
    return partitionSlices(idxs)
}

@JvmOverloads
fun writeRelation(
    trieWriter: ITrieWriter,
    relation: RelationReader,
    pageLimit: Int = 256,
) {
    val trieDataWriter = trieWriter.dataWriter
    val rowCopier = trieDataWriter.rowCopier(relation)
    val iidReader = relation.readerForName("xt\$iid")

    fun writeSubtree(depth: Int, sel: IntArray): Int =
        if (sel.size <= pageLimit) {
            for (idx in sel) rowCopier.copyRow(idx)

            val pos = trieWriter.writeLeaf()
            trieDataWriter.clear()
            pos
        } else {
            trieWriter.writeBranch(
                sel.partitionSelection(iidReader, depth)
                    .map { innerSel ->
                        if (innerSel == null) -1 else writeSubtree(depth + 1, innerSel)
                    }
                    .toIntArray())
        }

    writeSubtree(0, IntArray(relation.rowCount()) { idx -> idx })

    trieWriter.end()
}

