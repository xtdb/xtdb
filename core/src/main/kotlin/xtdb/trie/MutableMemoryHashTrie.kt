package xtdb.trie

import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary
import org.roaringbitmap.RoaringBitmap
import xtdb.arrow.VectorReader
import xtdb.trie.HashTrie.Companion.DEFAULT_LEVEL_BITS
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.function.IntUnaryOperator

private const val PAGE_LIMIT = 1024

// hashReader is assumed to be a FixedSizeBinary vector
// for iids this is ArrowType.FixedSizeBinary(16)
// for hashes this is ArrowType.Int
class MutableMemoryHashTrie(override val rootNode: Node, val hashReader: VectorReader, levelBits: Int) : BaseHashTrie<MutableMemoryHashTrie.Node, MutableMemoryHashTrie.Leaf>(levelBits) {

    private val max_level : Int

    init {
        max_level =  when(val type = hashReader.field.type) {
            is FixedSizeBinary -> type.byteWidth * 8 / LEVEL_BITS
            is ArrowType.Int -> type.bitWidth / LEVEL_BITS
            else -> throw IllegalArgumentException("Unsupported Arrow type for hashReader: ${hashReader.field.type}")
        }
    }

    sealed interface Node : HashTrie.Node<Node> {
        fun add(trie: MutableMemoryHashTrie, newIdx: Int): Node
        fun addIfNotPresent(trie: MutableMemoryHashTrie, hash: ByteArray, newIdx: Int, comparator: IntUnaryOperator, onAddition: Runnable): Pair<Int, Node?>

        fun findCandidates(trie: MutableMemoryHashTrie, hash: ByteArray): IntArray
        fun findValue(trie: MutableMemoryHashTrie, hash: ByteArray, comparator: IntUnaryOperator, removeOnMatch: Boolean): Int

        fun sortData(trie: MutableMemoryHashTrie)
    }

    @Suppress("unused")
    class Builder(private val hashReader: VectorReader) {
        private var pageLimit = PAGE_LIMIT
        private var levelBits = DEFAULT_LEVEL_BITS
        private var rootPath = ByteArray(0)

        fun setPageLimit(pageLimit: Int) = this.apply { this.pageLimit = pageLimit }
        fun setRootPath(path: ByteArray) = this.apply { this.rootPath = path }
        fun setLevelBits(levelBits: Int) = this.apply { this.levelBits = levelBits }
        fun build(): MutableMemoryHashTrie = MutableMemoryHashTrie(Leaf(pageLimit, rootPath), hashReader, levelBits)
    }

    operator fun plus(idx: Int) = MutableMemoryHashTrie(rootNode.add(this, idx), hashReader, LEVEL_BITS)

    private fun intToByteArray(hash: Int): ByteArray = ByteBuffer.allocate(Int.SIZE_BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(hash).array()

    fun addIfNotPresent(hash: Int, newIdx: Int, comparator: IntUnaryOperator, onAddition: Runnable): Pair<Int, MutableMemoryHashTrie>  {
        val (idx, node) = rootNode.addIfNotPresent(this, intToByteArray(hash), newIdx, comparator, onAddition)
        return Pair(idx, node?.let { MutableMemoryHashTrie(it, hashReader, LEVEL_BITS) } ?: this)
    }

    fun findCandidates (hash: ByteArray) : IntArray = rootNode.findCandidates(this, hash)

    fun findCandidates(hash: Int) : IntArray =
        findCandidates(intToByteArray(hash))

    // This assumes the trie has been compacted
    fun findValue (hash: ByteArray, comparator: IntUnaryOperator, removeOnMatch: Boolean) : Int =
        rootNode.findValue(this, hash, comparator, removeOnMatch)

    fun findValue(hash: Int, comparator: IntUnaryOperator, removeOnMatch: Boolean) : Int =
        findValue(intToByteArray(hash), comparator, removeOnMatch)

    @Suppress("unused")
    fun withIidReader(hashReader: VectorReader) = MutableMemoryHashTrie(rootNode, hashReader, LEVEL_BITS)

    fun sortData() {
       rootNode.sortData(this)
    }

    private fun bucketFor(idx: Int, level: Int, reusePtr: ArrowBufPointer): Int =
        this.bucketFor(hashReader.getPointer(idx, reusePtr), level).toInt()

    private fun compare(leftIdx: Int, rightIdx: Int, leftPtr: ArrowBufPointer, rightPtr: ArrowBufPointer): Int {
        val cmp =
            hashReader.getPointer(leftIdx, leftPtr)
                .compareTo(hashReader.getPointer(rightIdx, rightPtr))

        return if (cmp != 0) cmp else rightIdx compareTo leftIdx
    }


    class Branch(
        private val pageLimit: Int,
        override val path: ByteArray,
        override val hashChildren: Array<Node?>,
    ) : Node {
        private val addPtr = ArrowBufPointer()

        override fun add(trie: MutableMemoryHashTrie, newIdx: Int): Node {
            val bucket = trie.bucketFor(newIdx, path.size, addPtr)

            if (hashChildren[bucket] == null) {
                hashChildren[bucket] = Leaf(pageLimit, conjPath(path, bucket.toByte()))
            }

            hashChildren[bucket] = hashChildren[bucket]!!.add(trie, newIdx)

            return this
        }

        override fun addIfNotPresent(trie: MutableMemoryHashTrie, hash: ByteArray, newIdx: Int, comparator: IntUnaryOperator, onAddition: Runnable): Pair<Int, Node?> {
            val bucket = trie.bucketFor(hash, path.size).toInt()

            if (hashChildren[bucket] == null) {
                hashChildren[bucket] = Leaf(pageLimit, conjPath(path, bucket.toByte()))
            }

            val res = hashChildren[bucket]!!.addIfNotPresent(trie, hash, newIdx, comparator, onAddition)
            if (res.second == null) {
                return res
            }
            hashChildren[bucket] = res.second
            return Pair(res.first, this)
        }

        override fun findCandidates(trie: MutableMemoryHashTrie, hash: ByteArray): IntArray {
            val bucket = trie.bucketFor(hash, path.size).toInt()
            val child = hashChildren[bucket] ?: return IntArray(0)
            return child.findCandidates(trie, hash)
        }

        override fun findValue(trie: MutableMemoryHashTrie, hash: ByteArray, comparator: IntUnaryOperator, removeOnMatch: Boolean ): Int {
            val bucket = trie.bucketFor(hash, path.size).toInt()
            val child = hashChildren[bucket] ?: return -1
            return child.findValue(trie, hash, comparator, removeOnMatch)
        }

        override fun sortData(trie: MutableMemoryHashTrie) {
            hashChildren.map { child -> child?.sortData(trie) }

        }
    }

    class Leaf(
        private val pageLimit: Int,
        override val path: ByteArray,
        val data: IntArray = IntArray(pageLimit),
        private var dataCount: Int = 0,
        private var deletions: RoaringBitmap? = null
    ) : Node {

        override val hashChildren = null

        private fun split(trie: MutableMemoryHashTrie): Node {
            val res = Branch(pageLimit, path, arrayOfNulls(trie.LEVEL_WIDTH))
            for (i in 0 until dataCount) {
                res.add(trie, data[i])
            }
            return res
        }

        private fun binarySearch(trie: MutableMemoryHashTrie, hash: ByteBuffer): Int {
            var left = 0
            var right = dataCount - 1
            while (left < right) {
                val mid = (left + right) / 2
                val midHash = trie.hashReader.getBytes(data[mid])
                val cmp = midHash.compareTo(hash)
                if (cmp < 0) left = mid + 1 else right = mid
            }
            if (left < dataCount) {
                val midHash = trie.hashReader.getBytes(data[left])
                if (midHash.compareTo(hash) == 0) return left
            }
            return -1
        }

        override fun add(trie: MutableMemoryHashTrie, newIdx: Int): Node {
            if (dataCount >= pageLimit) {
                return if (path.size < trie.max_level) {
                    split(trie).add(trie, newIdx)
                } else {
                    val newPageLimit = pageLimit * 2
                    val newData = IntArray(newPageLimit)
                    data.copyInto(newData)
                    newData[dataCount++] = newIdx
                    return Leaf(newPageLimit, path, newData, dataCount, deletions)
                }
            }
            data[dataCount++] = newIdx
            return this
        }

        override fun addIfNotPresent(trie: MutableMemoryHashTrie, hash: ByteArray, newIdx: Int, comparator: IntUnaryOperator, onAddition: Runnable): Pair<Int, Node?> {
            val bb = ByteBuffer.wrap(hash)
            // can't use binary search here because we might not be sorted
            for (i in 0 until dataCount) {
                val testIdx = data[i]
                if (trie.hashReader.getBytes(testIdx).compareTo(bb) == 0 && comparator.applyAsInt(testIdx) == 1) {
                    return Pair(testIdx, null)
                }
            }
            onAddition.run()
            return Pair(newIdx, add(trie, newIdx))
        }

        override fun findCandidates(trie: MutableMemoryHashTrie, hash: ByteArray): IntArray {
            val bb = ByteBuffer.wrap(hash)
            val idx = binarySearch(trie, bb)
            if (idx < 0) return IntArray(0)
            return data.take(dataCount).drop(idx).takeWhile{ trie.hashReader.getBytes(it).compareTo(bb) == 0 }.toIntArray()
        }

        override fun findValue(trie: MutableMemoryHashTrie, hash: ByteArray, comparator: IntUnaryOperator, removeOnMatch: Boolean): Int {
            val bb = ByteBuffer.wrap(hash)
            val idx = binarySearch(trie, bb)
            if (idx < 0) return -1
            val data = when (deletions) {
                null -> data.take(dataCount).drop(idx).takeWhile { trie.hashReader.getBytes(it).compareTo(bb) == 0 } .toIntArray()
                else -> data.take(dataCount).drop(idx).takeWhile { trie.hashReader.getBytes(it).compareTo(bb) == 0 } .filter { !deletions!!.contains(it) }.toIntArray()
            }
            for(testIdx in data) {
                if (comparator.applyAsInt(testIdx) == 1) {
                    if (removeOnMatch) {
                        deletions = (deletions ?: RoaringBitmap()).apply { add(testIdx) }
                    }
                    return testIdx
                }
            }
            return -1
        }

        override fun sortData(trie: MutableMemoryHashTrie) {
            if (dataCount == 0) return
            val leftPtr = ArrowBufPointer()
            val rightPtr = ArrowBufPointer()
            val tmp = data.take(dataCount).toTypedArray()
            tmp.sortWith { leftIdx, rightIdx -> trie.compare(leftIdx, rightIdx, leftPtr, rightPtr) }
            tmp.forEachIndexed { index, i -> data[index] = i }
        }
    }

    companion object {
        @JvmStatic
        fun builder(hashReader: VectorReader) = Builder(hashReader)

        @JvmStatic
        @Suppress("unused")
        fun emptyTrie(hashReader: VectorReader) = builder(hashReader).build()

        private fun conjPath(path: ByteArray, idx: Byte): ByteArray {
            val currentPathLength = path.size
            val childPath = ByteArray(currentPathLength + 1)
            System.arraycopy(path, 0, childPath, 0, currentPathLength)
            childPath[currentPathLength] = idx
            return childPath
        }
    }
}
