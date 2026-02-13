package xtdb.trie

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.arrow.VectorReader
import xtdb.trie.proto.*
import xtdb.trie.proto.HashTrie as HashTrieProto

private const val LOG_LIMIT = 64
private const val PAGE_LIMIT = 1024
private const val MAX_LEVEL = 64

class MemoryHashTrie(
    override val rootNode: Node, val iidReader: VectorReader, val logLimit: Int, val pageLimit: Int
) : HashTrie<MemoryHashTrie.Leaf> {

    private val bucketer = Bucketer()

    sealed interface Node : HashTrie.Node<Leaf> {
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
        fun build(): MemoryHashTrie =
            MemoryHashTrie(Leaf(logLimit, rootPath), iidReader, logLimit, pageLimit)
    }

    operator fun plus(idx: Int) = MemoryHashTrie(rootNode.add(this, idx), iidReader, logLimit, pageLimit)

    fun addRange(startIdx: Int, count: Int): MemoryHashTrie {
        var result = this
        for (i in startIdx until startIdx + count) {
            result += i
        }
        return result
    }

    @Suppress("unused")
    fun withIidReader(iidReader: VectorReader) = MemoryHashTrie(rootNode, iidReader, logLimit, pageLimit)

    fun compactLogs() = MemoryHashTrie(rootNode = rootNode.compactLogs(this), iidReader, logLimit, pageLimit)

    private fun bucketFor(idx: Int, level: Int, reusePtr: ArrowBufPointer): Int =
        bucketer.bucketFor(iidReader.getPointer(idx, reusePtr), level).toInt()

    private fun compare(leftIdx: Int, rightIdx: Int, leftPtr: ArrowBufPointer, rightPtr: ArrowBufPointer): Int {
        val cmp =
            iidReader.getPointer(leftIdx, leftPtr)
                .compareTo(iidReader.getPointer(rightIdx, rightPtr))

        return if (cmp != 0) cmp else rightIdx compareTo leftIdx
    }

    class Branch(
        override val path: ByteArray,
        override val hashChildren: List<Node?>,
    ) : Node {
        private val addPtr = ArrowBufPointer()

        override fun add(trie: MemoryHashTrie, newIdx: Int): Node {
            val bucket = trie.bucketFor(newIdx, path.size, addPtr)

            val newChildren = List(hashChildren.size) { childIdx ->
                var child = hashChildren[childIdx]
                if (bucket == childIdx) {
                    child = child ?: Leaf(trie.logLimit, conjPath(path, childIdx.toByte()))
                    child = child.add(trie, newIdx)
                }
                child
            }

            return Branch(path, newChildren)
        }

        override fun compactLogs(trie: MemoryHashTrie) =
            Branch(path, hashChildren.map { child -> child?.compactLogs(trie) })
    }

    class Leaf(
        logLimit: Int,
        override val path: ByteArray,
        val data: IntArray = IntArray(0),
        val log: IntArray = IntArray(logLimit),
        private val logCount: Int = 0,
        // Volatile: concurrent readers may race on mergeSort; the result is deterministic
        // so the worst case is redundant computation, but without volatile a reader on a
        // weak memory model might never see the cached value.
        @Volatile private var sortedData: IntArray? = null
    ) : Node {

        override val hashChildren = null

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
            val entryGroups = arrayOfNulls<IntArrayList>(trie.bucketer.levelWidth)
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

            val data = if (sortedData != null) sortedData as IntArray else mergeSort(
                trie,
                data,
                sortLog(trie, log, logCount),
                logCount
            )
            val log = IntArray(trie.logLimit)
            val logCount = 0

            return if (data.size > trie.pageLimit && path.size < MAX_LEVEL) {
                val childBuckets = idxBuckets(trie, data, path)

                val childNodes = childBuckets
                    .mapIndexed<IntArray?, Node?> { childIdx, childBucket ->
                        if (childBucket == null) null
                        else
                            Leaf(trie.pageLimit, conjPath(path, childIdx.toByte()), childBucket)
                    }

                Branch(path, childNodes)

            } else Leaf(trie.pageLimit, path, data, log, logCount)
        }

        override fun add(trie: MemoryHashTrie, newIdx: Int): Node {
            var logCount = logCount
            log[logCount++] = newIdx
            val newLeaf = Leaf(trie.pageLimit, path, data, log, logCount)

            return if (logCount == trie.logLimit) newLeaf.compactLogs(trie) else newLeaf
        }
    }

    private fun Node?.asProto(): HashTrieNode =
        hashTrieNode {
            when (this@asProto) {
                is Branch -> branch = branch { children.addAll(hashChildren.map { it.asProto() }) }
                is Leaf -> leaf = leaf { data.addAll(this@asProto.data.asIterable()) }
                null -> Unit
            }
        }

    val asProto: ByteArray
        get() = hashTrie {
            logLimit = this@MemoryHashTrie.logLimit
            pageLimit = this@MemoryHashTrie.pageLimit
            rootNode = compactLogs().rootNode.asProto()
        }.toByteArray()

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

        private fun HashTrieNode.decode(logLimit: Int): Node? =
            when (nodeCase) {
                HashTrieNode.NodeCase.BRANCH ->
                    Branch(path.toByteArray(), branch.childrenList.map { it.decode(logLimit) })

                HashTrieNode.NodeCase.LEAF ->
                    Leaf(logLimit, path.toByteArray(), leaf.dataList.toIntArray())

                HashTrieNode.NodeCase.NODE_NOT_SET -> null
            }

        @JvmStatic
        fun fromProto(bytes: ByteArray, iidReader: VectorReader): MemoryHashTrie {
            val msg = HashTrieProto.parseFrom(bytes)

            return MemoryHashTrie(
                msg.rootNode.decode(msg.logLimit)!!,
                iidReader, msg.logLimit, msg.pageLimit
            )
        }
    }
}
