package xtdb.trie

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.trie.HashTrie.Companion.bucketFor
import xtdb.vector.IVectorReader

private const val LOG_LIMIT = 64
private const val PAGE_LIMIT = 1024
private const val MAX_LEVEL = 64

data class LiveHashTrie(override val rootNode: Node, val iidReader: IVectorReader) : HashTrie<LiveHashTrie.Node> {
    interface Node : HashTrie.Node<Node> {
        fun add(trie: LiveHashTrie, newIdx: Int): Node

        fun compactLogs(trie: LiveHashTrie): Node
    }

    @Suppress("unused")
    class Builder(private val iidReader: IVectorReader) {
        private var logLimit = LOG_LIMIT
        private var pageLimit = PAGE_LIMIT
        private var rootPath = ByteArray(0)

        fun setLogLimit(logLimit: Int) = this.apply { this.logLimit = logLimit }
        fun setPageLimit(pageLimit: Int) = this.apply { this.pageLimit = pageLimit }
        fun setRootPath(path: ByteArray) = this.apply { this.rootPath = path }
        fun build(): LiveHashTrie = LiveHashTrie(Leaf(logLimit, pageLimit, rootPath), iidReader)
    }

    fun add(idx: Int) = copy(rootNode = rootNode.add(this, idx))

    @Suppress("unused")
    fun withIidReader(iidReader: IVectorReader) = copy(iidReader = iidReader)

    fun compactLogs() = copy(rootNode = rootNode.compactLogs(this))

    private fun bucketFor(idx: Int, level: Int): Int =
        bucketFor(iidReader.getPointer(idx, BUCKET_BUF_PTR.get()), level).toInt()

    private fun compare(leftIdx: Int, rightIdx: Int): Int {
        val cmp =
            iidReader.getPointer(leftIdx, LEFT_BUF_PTR.get())
                .compareTo(iidReader.getPointer(rightIdx, RIGHT_BUF_PTR.get()))

        return if (cmp != 0) cmp else rightIdx compareTo leftIdx
    }

    class Branch(
        private val logLimit: Int,
        private val pageLimit: Int,
        override val path: ByteArray,
        override val children: Array<Node?>,
    ) : Node {
        override fun add(trie: LiveHashTrie, newIdx: Int): Node {
            val bucket = trie.bucketFor(newIdx, path.size)

            val newChildren = children.indices
                .map { childIdx ->
                    var child = children[childIdx]
                    if (bucket == childIdx) {
                        child = child ?: Leaf(logLimit, pageLimit, conjPath(path, childIdx.toByte()))
                        child = child.add(trie, newIdx)
                    }
                    child
                }.toTypedArray()

            return Branch(logLimit, pageLimit, path, newChildren)
        }

        override fun compactLogs(trie: LiveHashTrie) =
            Branch(logLimit, pageLimit, path, children.map { child -> child?.compactLogs(trie) }.toTypedArray())
    }

    class Leaf(
        private val logLimit: Int,
        private val pageLimit: Int,
        override val path: ByteArray,
        val data: IntArray = IntArray(0),
        val log: IntArray = IntArray(logLimit),
        private val logCount: Int = 0,
    ) : Node {

        override val children = null

        fun mergeSort(trie: LiveHashTrie): IntArray =
            mergeSort(trie, data, sortLog(trie, log, logCount), logCount)

        private fun mergeSort(trie: LiveHashTrie, data: IntArray, log: IntArray, logCount: Int): IntArray {
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

                if (trie.compare(dataKey, logKey) < 0) {
                    res.add(dataKey)
                    dataIdx++
                } else {
                    res.add(logKey)
                    logIdx++
                }
            }

            return res.toArray()
        }

        private fun sortLog(trie: LiveHashTrie, log: IntArray, logCount: Int) =
            log.take(logCount).sortedWith(trie::compare).toIntArray()

        private fun idxBuckets(trie: LiveHashTrie, idxs: IntArray, path: ByteArray): Array<IntArray?> {
            val entryGroups = arrayOfNulls<IntArrayList>(LEVEL_WIDTH)

            for (i in idxs) {
                val groupIdx = trie.bucketFor(i, path.size)
                val group = entryGroups[groupIdx] ?: IntArrayList().also { entryGroups[groupIdx] = it }
                group.add(i)
            }

            return entryGroups.map { b -> b?.toArray() }.toTypedArray()
        }

        override fun compactLogs(trie: LiveHashTrie): Node {
            if (logCount == 0) return this

            val data = mergeSort(trie, data, sortLog(trie, log, logCount), logCount)
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

        override fun add(trie: LiveHashTrie, newIdx: Int): Node {
            var logCount = logCount
            log[logCount++] = newIdx
            val newLeaf = Leaf(logLimit, pageLimit, path, data, log, logCount)

            return if (logCount == logLimit) newLeaf.compactLogs(trie) else newLeaf
        }
    }

    companion object {
        @JvmStatic
        fun builder(iidReader: IVectorReader) = Builder(iidReader)

        @JvmStatic
        @Suppress("unused")
        fun emptyTrie(iidReader: IVectorReader) = builder(iidReader).build()

        private val BUCKET_BUF_PTR: ThreadLocal<ArrowBufPointer> = ThreadLocal.withInitial(::ArrowBufPointer)
        private val LEFT_BUF_PTR: ThreadLocal<ArrowBufPointer> = ThreadLocal.withInitial(::ArrowBufPointer)
        private val RIGHT_BUF_PTR: ThreadLocal<ArrowBufPointer> = ThreadLocal.withInitial(::ArrowBufPointer)

        private fun conjPath(path: ByteArray, idx: Byte): ByteArray {
            val currentPathLength = path.size
            val childPath = ByteArray(currentPathLength + 1)
            System.arraycopy(path, 0, childPath, 0, currentPathLength)
            childPath[currentPathLength] = idx
            return childPath
        }
    }
}
