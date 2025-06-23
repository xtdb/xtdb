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
    }

    @Suppress("unused")
    class Builder(private val iidReader: VectorReader) {
        private var logLimit = LOG_LIMIT
        private var pageLimit = PAGE_LIMIT
        private var rootPath = ByteArray(0)

        fun setLogLimit(logLimit: Int) = this.apply { this.logLimit = logLimit }
        fun setPageLimit(pageLimit: Int) = this.apply { this.pageLimit = pageLimit }
        fun setRootPath(path: ByteArray) = this.apply { this.rootPath = path }
        fun build(): MemoryHashTrie = MemoryHashTrie(Leaf(logLimit, pageLimit, rootPath), iidReader)
    }

    operator fun plus(idx: Int) = MemoryHashTrie(rootNode.add(this, idx), iidReader)

    @Suppress("unused")
    fun withIidReader(iidReader: VectorReader) = MemoryHashTrie(rootNode, iidReader)

    fun compactLogs() = MemoryHashTrie(rootNode = rootNode.compactLogs(this), iidReader)

    private fun bucketFor(idx: Int, level: Int, reusePtr: ArrowBufPointer): Int =
        bucketFor(iidReader.getPointer(idx, reusePtr), level).toInt()

    private fun compare(leftIdx: Int, rightIdx: Int, leftPtr: ArrowBufPointer, rightPtr: ArrowBufPointer): Int {
        val cmp =
            iidReader.getPointer(leftIdx, leftPtr)
                .compareTo(iidReader.getPointer(rightIdx, rightPtr))

        return if (cmp != 0) cmp else rightIdx compareTo leftIdx
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

        override fun add(trie: MemoryHashTrie, newIdx: Int): Node {
            var logCount = logCount
            log[logCount++] = newIdx
            val newLeaf = Leaf(logLimit, pageLimit, path, data, log, logCount)

            return if (logCount == logLimit) newLeaf.compactLogs(trie) else newLeaf
        }
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
