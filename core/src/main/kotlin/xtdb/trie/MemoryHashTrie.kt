package xtdb.trie

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.arrow.VectorReader
import xtdb.trie.HashTrie.Companion.LEVEL_WIDTH
import xtdb.trie.HashTrie.Companion.bucketFor

private const val LOG_LIMIT = 64
private const val PAGE_LIMIT = 1024
private const val MAX_LEVEL = 64

class MemoryHashTrie(override val rootNode: Node, val iidReader: VectorReader) : HashTrie<MemoryHashTrie.Node, MemoryHashTrie.Leaf> {

    sealed interface Node : HashTrie.Node<Node> {
        fun add(trie: MemoryHashTrie, newIdx: Int): Node
        fun compactLogs(trie: MemoryHashTrie): Node
        fun transient(trie: MemoryHashTrie): Node
        fun persist(trie: MemoryHashTrie): Node
    }

    @Suppress("unused")
    class Builder(private val iidReader: VectorReader) {
        private var logLimit = LOG_LIMIT
        private var pageLimit = PAGE_LIMIT
        private var rootPath = ByteArray(0)
        private var transient = false

        fun setLogLimit(logLimit: Int) = this.apply { this.logLimit = logLimit }
        fun setPageLimit(pageLimit: Int) = this.apply { this.pageLimit = pageLimit }
        fun setRootPath(path: ByteArray) = this.apply { this.rootPath = path }
        fun setTransient(transient: Boolean) = this.apply { this.transient = transient }
        fun build(): MemoryHashTrie =
            MemoryHashTrie(
                if (transient) LeafTransient(pageLimit, rootPath)
                else Leaf(logLimit, pageLimit, rootPath),
                iidReader)
    }

    operator fun plus(idx: Int) = MemoryHashTrie(rootNode.add(this, idx), iidReader)

    @Suppress("unused")
    fun withIidReader(iidReader: VectorReader) = MemoryHashTrie(rootNode, iidReader)

    fun compactLogs() = when(rootNode) {
        is Leaf, is Branch -> MemoryHashTrie(rootNode = rootNode.compactLogs(this), iidReader)
        else -> throw UnsupportedOperationException("Cannot compact logs on a transient trie")
    }

    private fun bucketFor(idx: Int, level: Int, reusePtr: ArrowBufPointer): Int =
        bucketFor(iidReader.getPointer(idx, reusePtr), level).toInt()

    private fun compare(leftIdx: Int, rightIdx: Int, leftPtr: ArrowBufPointer, rightPtr: ArrowBufPointer): Int {
        val cmp =
            iidReader.getPointer(leftIdx, leftPtr)
                .compareTo(iidReader.getPointer(rightIdx, rightPtr))

        return if (cmp != 0) cmp else rightIdx compareTo leftIdx
    }

    fun asTransient(): MemoryHashTrie {
        if (rootNode is LeafTransient || rootNode is BranchTransient) return this
        return MemoryHashTrie(rootNode.transient(this), iidReader)
    }

    fun asPersistent(): MemoryHashTrie {
        if (rootNode is Leaf || rootNode is Branch) return this
        return MemoryHashTrie(rootNode.persist(this), iidReader)
    }


    class Branch(
        private val logLimit: Int,
        private val pageLimit: Int,
        override val path: ByteArray,
        override val iidChildren: Array<Node?>,
    ) : Node {
        private val addPtr = ArrowBufPointer()

        override fun add(trie: MemoryHashTrie, newIdx: Int): Node {
            val bucket = trie.bucketFor(newIdx, path.size, addPtr)

            val newChildren = iidChildren.indices
                .map { childIdx ->
                    var child = iidChildren[childIdx]
                    if (bucket == childIdx) {
                        child = child ?: Leaf(logLimit, pageLimit, conjPath(path, childIdx.toByte()))
                        child = child.add(trie, newIdx)
                    }
                    child
                }.toTypedArray()

            return Branch(logLimit, pageLimit, path, newChildren)
        }

        override fun compactLogs(trie: MemoryHashTrie) =
            Branch(logLimit, pageLimit, path, iidChildren.map { child -> child?.compactLogs(trie) }.toTypedArray())

        override fun transient(trie: MemoryHashTrie): Node =
            BranchTransient(pageLimit, path, iidChildren.map { child -> child?.transient(trie) }.toTypedArray())

        override fun persist(trie: MemoryHashTrie): Node {
            TODO("Not yet implemented")
        }
    }

    class BranchTransient(
        private val pageLimit: Int,
        override val path: ByteArray,
        override val iidChildren: Array<Node?>,
    ) : Node {
        private val addPtr = ArrowBufPointer()

        override fun add(trie: MemoryHashTrie, newIdx: Int): Node {
            val bucket = trie.bucketFor(newIdx, path.size, addPtr)

            if (iidChildren[bucket] == null) {
                iidChildren[bucket] = LeafTransient(pageLimit, conjPath(path, bucket.toByte()))
            }

            iidChildren[bucket] = iidChildren[bucket]!!.add(trie, newIdx)

            return this
        }

        override fun compactLogs(trie: MemoryHashTrie): Node {
            TODO("Not yet implemented")
        }

        override fun transient(trie: MemoryHashTrie): Node {
            TODO("Not yet implemented")
        }

        override fun persist(trie: MemoryHashTrie): Node =
            Branch(LOG_LIMIT, pageLimit, path, iidChildren.map { child -> child?.persist(trie) }.toTypedArray())

    }

    class Leaf(
        private val logLimit: Int,
        private val pageLimit: Int,
        override val path: ByteArray,
        val data: IntArray = IntArray(0),
        val log: IntArray = IntArray(logLimit),
        private val logCount: Int = 0,
        private var sortedData: IntArray? = null
    ) : Node {

        override val iidChildren = null

        fun mergeSort(trie: MemoryHashTrie): IntArray {
            if (log.isEmpty()) return data
            return sortedData ?: mergeSort(trie, data, sortLog(trie, log, logCount), logCount)
        }

        private fun mergeSort(trie: MemoryHashTrie, data: IntArray, log: IntArray, logCount: Int): IntArray {
            val leftPtr = ArrowBufPointer()
            val logPtr = ArrowBufPointer()
            val dataCount = data.size

            val res = IntArrayList(data.size + logCount)
            var dataIdx = 0
            var logIdx = 0

            while (true) {
                if (dataIdx == dataCount) {
                    for (idx in logIdx..<logCount) res.add(log[idx])
                    break
                }

                if (logIdx == logCount) {
                    for (idx in dataIdx..<dataCount) res.add(data[idx])
                    break
                }

                val dataKey = data[dataIdx]
                val logKey = log[logIdx]

                if (trie.compare(dataKey, logKey, leftPtr, logPtr) < 0) {
                    res.add(dataKey)
                    dataIdx++
                } else {
                    res.add(logKey)
                    logIdx++
                }
            }

            return res.toArray().also { sortedData = it }
        }

        private fun sortLog(trie: MemoryHashTrie, log: IntArray, logCount: Int): IntArray {
            val leftPtr = ArrowBufPointer()
            val rightPtr = ArrowBufPointer()
            return log.take(logCount).sortedWith { leftKey: Int, rightKey: Int ->
                trie.compare(leftKey, rightKey, leftPtr, rightPtr)
            }.toIntArray()
        }


        private fun idxBuckets(trie: MemoryHashTrie, idxs: IntArray, path: ByteArray): Array<IntArray?> {
            val entryGroups = arrayOfNulls<IntArrayList>(LEVEL_WIDTH)
            val ptr = ArrowBufPointer()

            for (i in idxs) {
                val groupIdx = trie.bucketFor(i, path.size, ptr)
                val group = entryGroups[groupIdx] ?: IntArrayList().also { entryGroups[groupIdx] = it }
                group.add(i)
            }

            return entryGroups.map { b -> b?.toArray() }.toTypedArray()
        }

        override fun compactLogs(trie: MemoryHashTrie): Node {
            if (logCount == 0) return this

            val data = if (sortedData != null) sortedData as IntArray else mergeSort(trie, data, sortLog(trie, log, logCount), logCount)
            val log = IntArray(logLimit)
            val logCount = 0

            return if (data.size > pageLimit && path.size < MAX_LEVEL) {
                val childBuckets = idxBuckets(trie, data, path)

                val childNodes = childBuckets
                    .mapIndexed<IntArray?, Node?> { childIdx, childBucket ->
                        if (childBucket == null) null
                        else
                            Leaf(logLimit, pageLimit, conjPath(path, childIdx.toByte()), childBucket)
                    }.toTypedArray()

                Branch(logLimit, pageLimit, path, childNodes)

            } else Leaf(logLimit, pageLimit, path, data, log, logCount)
        }

        override fun transient(trie: MemoryHashTrie): Node =
            when(val compacted = compactLogs(trie)) {
                is Leaf -> {
                    val data = IntArray(pageLimit)
                    compacted.data.copyInto(data)
                    LeafTransient(pageLimit, path, data, compacted.data.size)
                }
                is Branch -> compacted.transient(trie)
                else -> throw IllegalStateException("Unexpected node type after compacting logs: $compacted")
            }

        override fun persist(trie: MemoryHashTrie): Node {
            TODO("Not yet implemented")
        }

        override fun add(trie: MemoryHashTrie, newIdx: Int): Node {
            var logCount = logCount
            log[logCount++] = newIdx
            val newLeaf = Leaf(logLimit, pageLimit, path, data, log, logCount)

            return if (logCount == logLimit) newLeaf.compactLogs(trie) else newLeaf
        }
    }

    class LeafTransient(
        private val pageLimit: Int,
        override val path: ByteArray,
        val data: IntArray = IntArray(pageLimit),
        private var dataCount: Int = 0,
    ) : Node {

        override val iidChildren = null

        private fun split(trie: MemoryHashTrie): Node {
            val res = BranchTransient(pageLimit, path, arrayOfNulls(LEVEL_WIDTH))
            for (i in 0 until dataCount) {
                res.add(trie, data[i])
            }
            return res
        }

        override fun add(trie: MemoryHashTrie, newIdx: Int): Node {
            if (dataCount >= pageLimit) {
                return if (path.size < MAX_LEVEL) {
                    split(trie).add(trie, newIdx)
                } else {
                    val newPageLimit = pageLimit * 2
                    val newData = IntArray(newPageLimit)
                    data.copyInto(newData)
                    newData[dataCount++] = newIdx
                    return LeafTransient(newPageLimit, path, newData, dataCount)
                }
            }
            data[dataCount++] = newIdx
            return this
        }

        override fun compactLogs(trie: MemoryHashTrie): Node {
            TODO("Not yet implemented")
        }

        override fun transient(trie: MemoryHashTrie): Node {
            TODO("Not yet implemented")
        }

        private fun sortData(trie: MemoryHashTrie) : IntArray {
            if (dataCount == 0) return IntArray(0)
            val leftPtr = ArrowBufPointer()
            val rightPtr = ArrowBufPointer()
            val tmp = data.take(dataCount).toTypedArray()
            tmp.sortWith { leftIdx, rightIdx -> trie.compare(leftIdx, rightIdx, leftPtr, rightPtr) }
            return tmp.toIntArray()
        }

        override fun persist(trie: MemoryHashTrie): Node =
            Leaf(LOG_LIMIT, pageLimit, path, sortData(trie))
    }

    companion object {
        @JvmStatic
        fun builder(iidReader: VectorReader) = Builder(iidReader)

        @JvmStatic
        @Suppress("unused")
        fun emptyTrie(iidReader: VectorReader) = builder(iidReader).build()

        private fun conjPath(path: ByteArray, idx: Byte): ByteArray {
            val currentPathLength = path.size
            val childPath = ByteArray(currentPathLength + 1)
            System.arraycopy(path, 0, childPath, 0, currentPathLength)
            childPath[currentPathLength] = idx
            return childPath
        }
    }
}
