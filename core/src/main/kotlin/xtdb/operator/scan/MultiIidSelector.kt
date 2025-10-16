package xtdb.operator.scan

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.RelationReader
import xtdb.operator.SelectionSpec
import xtdb.trie.Bucketer
import java.util.*

class MultiIidSelector(private val iids: SortedSet<ByteArray>) : SelectionSpec {

    private fun select(readRelation: RelationReader, iids: SortedSet<ByteArray>): IntArray {
        val res = IntArrayList()
        val iidReader = readRelation["_iid"]

        repeat(readRelation.rowCount) { rowIdx ->
            if (iidReader[rowIdx] in iids) {
                res.add(rowIdx)
            }
        }

        return res.toArray()
    }

    override fun select(
        allocator: BufferAllocator, readRelation: RelationReader, schema: Map<String, Any>, params: RelationReader
    ) = select(readRelation, iids)

    private val bucketer = Bucketer.DEFAULT

    fun select(readRelation: RelationReader, path: ByteArray): IntArray {
        val iids =
            bucketer.incrementPath(path)?.let { iids.subSet(bucketer.startIid(path), bucketer.startIid(it)) }
                ?: iids.tailSet(path)

        return select(readRelation, iids)
    }
}