package xtdb.trie

import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.memory.util.ByteFunctionHelpers
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary
import org.roaringbitmap.RoaringBitmap
import xtdb.arrow.VectorReader
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.function.IntConsumer
import java.util.function.IntUnaryOperator

private const val PAGE_LIMIT = 1024

// hashReader is assumed to be a fixed-width vector
// for iids this is ArrowType.FixedSizeBinary(16)
// for hashes this is ArrowType.Int
class MutableMemoryHashTrie(override val rootNode: Node, val hashReader: VectorReader, levelBits: Int) : HashTrie<MutableMemoryHashTrie.Node, MutableMemoryHashTrie.Leaf> {
    private val bucketer = Bucketer(levelBits)

    private val hashByteWidth : Int = when(val type = hashReader.field.type) {
        is FixedSizeBinary -> type.byteWidth
        is ArrowType.Int -> 4
        else -> throw IllegalArgumentException("Unsupported Arrow type for hashReader: ${hashReader.field.type}")
    }
    private val maxLevel : Int = when(val type = hashReader.field.type) {
        is FixedSizeBinary -> type.byteWidth * 8 / bucketer.levelBits
        is ArrowType.Int -> type.bitWidth / bucketer.levelBits
        else -> throw IllegalArgumentException("Unsupported Arrow type for hashReader: ${hashReader.field.type}")
    }
    private val reusePtr1 = ArrowBufPointer()
    private val reusePtr2 = ArrowBufPointer()

    sealed interface Node : HashTrie.Node<Node> {
        fun add(trie: MutableMemoryHashTrie, newIdx: Int): Node
        fun addIfNotPresent(trie: MutableMemoryHashTrie, hash: ByteArray, newIdx: Int, comparator: IntUnaryOperator, onAddition: Runnable): Pair<Int, Node?>

        fun forEachMatch(trie: MutableMemoryHashTrie, hash: ByteArray, c: IntConsumer)
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

    operator fun plus(idx: Int) = MutableMemoryHashTrie(rootNode.add(this, idx), hashReader, bucketer.levelBits)

    private fun intToByteArray(hash: Int): ByteArray = ByteBuffer.allocate(Int.SIZE_BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(hash).array()

    fun addIfNotPresent(hash: Int, newIdx: Int, comparator: IntUnaryOperator, onAddition: Runnable): Pair<Int, MutableMemoryHashTrie>  {
        val (idx, node) = rootNode.addIfNotPresent(this, intToByteArray(hash), newIdx, comparator, onAddition)
        return Pair(idx, node?.let { MutableMemoryHashTrie(it, hashReader, bucketer.levelBits) } ?: this)
    }

    fun forEachMatch(hash: ByteArray, c: IntConsumer) = rootNode.forEachMatch(this, hash, c)

    fun forEachMatch(hash: Int, c: IntConsumer) = forEachMatch(intToByteArray(hash), c)

    // This assumes the trie has been compacted
    fun findValue (hash: ByteArray, comparator: IntUnaryOperator, removeOnMatch: Boolean) : Int =
        rootNode.findValue(this, hash, comparator, removeOnMatch)

    fun findValue(hash: Int, comparator: IntUnaryOperator, removeOnMatch: Boolean) : Int =
        findValue(intToByteArray(hash), comparator, removeOnMatch)

    @Suppress("unused")
    fun withIidReader(hashReader: VectorReader) = MutableMemoryHashTrie(rootNode, hashReader, bucketer.levelBits)

    fun sortData() {
       rootNode.sortData(this)
    }

    private fun bucketFor(idx: Int, level: Int, reusePtr: ArrowBufPointer): Int =
        bucketer.bucketFor(hashReader.getPointer(idx, reusePtr), level).toInt()

    private fun compare(leftIdx: Int, rightIdx: Int, leftPtr: ArrowBufPointer, rightPtr: ArrowBufPointer): Int {
        val cmp =
            hashReader.getPointer(leftIdx, leftPtr)
                .compareTo(hashReader.getPointer(rightIdx, rightPtr))

        return if (cmp != 0) cmp else rightIdx compareTo leftIdx
    }

    private fun compare(idx: Int, ptr: ArrowBufPointer, hash: ByteArray) : Int {
        val buf = hashReader.getPointer(idx, ptr).buf
        val index = idx * hashByteWidth
        return ByteFunctionHelpers.compare(buf, index, index + hashByteWidth, hash, 0, hash.size)
    }

    class Branch(
        private val pageLimit: Int,
        override val path: ByteArray,
        override val hashChildren: Array<Node?>,
    ) : Node {

        override fun add(trie: MutableMemoryHashTrie, newIdx: Int): Node {
            val bucket = trie.bucketFor(newIdx, path.size, trie.reusePtr1)

            if (hashChildren[bucket] == null) {
                hashChildren[bucket] = Leaf(pageLimit, conjPath(path, bucket.toByte()))
            }

            hashChildren[bucket] = hashChildren[bucket]!!.add(trie, newIdx)

            return this
        }

        override fun addIfNotPresent(trie: MutableMemoryHashTrie, hash: ByteArray, newIdx: Int, comparator: IntUnaryOperator, onAddition: Runnable): Pair<Int, Node?> {
            val bucket = trie.bucketer.bucketFor(hash, path.size).toInt()

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

        override fun forEachMatch(trie: MutableMemoryHashTrie, hash: ByteArray, c: IntConsumer) {
            val bucket = trie.bucketer.bucketFor(hash, path.size).toInt()
            val child = hashChildren[bucket] ?: return
            return child.forEachMatch(trie, hash, c)
        }

        override fun findValue(trie: MutableMemoryHashTrie, hash: ByteArray, comparator: IntUnaryOperator, removeOnMatch: Boolean ): Int {
            val bucket = trie.bucketer.bucketFor(hash, path.size).toInt()
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
        private var deletions: RoaringBitmap? = null,
        private var sorted: Boolean = false
    ) : Node {

        override val hashChildren = null

        private fun split(trie: MutableMemoryHashTrie): Node {
            val res = Branch(pageLimit, path, arrayOfNulls(trie.bucketer.levelWidth))
            for (i in 0 until dataCount) {
                res.add(trie, data[i])
            }
            return res
        }

        private fun binarySearch(trie: MutableMemoryHashTrie, hash: ByteArray): Int {
            var left = 0
            var right = dataCount - 1
            while (left < right) {
                val mid = (left + right) / 2
                val cmp = trie.compare(data[mid], trie.reusePtr1, hash)
                if (cmp < 0) left = mid + 1 else right = mid
            }
            if (left < dataCount) {
                if (trie.compare(data[left], trie.reusePtr1, hash) == 0) return left
            }
            return -1
        }

        override fun add(trie: MutableMemoryHashTrie, newIdx: Int): Node {
            if (dataCount >= pageLimit) {
                return if (path.size < trie.maxLevel) {
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
            // can't use binary search here because we might not be sorted
            for (i in 0 until dataCount) {
                val testIdx = data[i]
                if (trie.compare(testIdx, trie.reusePtr1, hash) == 0 && comparator.applyAsInt(testIdx) == 1) {
                    return Pair(testIdx, null)
                }
            }
            onAddition.run()
            return Pair(newIdx, add(trie, newIdx))
        }

        override fun sortData(trie: MutableMemoryHashTrie) {
            if (dataCount == 0) return
            val tmp = data.take(dataCount).toTypedArray()
            tmp.sortWith { leftIdx, rightIdx -> trie.compare(leftIdx, rightIdx, trie.reusePtr1, trie.reusePtr2) }
            tmp.forEachIndexed { index, i -> data[index] = i }
            sorted = true
        }


        // Beware that this method doesn't honour deletions (see findValue below)
        // Any operator that uses these methods through RelationMapProber either uses one or the other, hence no need to check deletions in this case.
        override fun forEachMatch(trie: MutableMemoryHashTrie, hash: ByteArray, c: IntConsumer) {
            if (dataCount <= 16) {
                for (i in 0 until dataCount) {
                    if (trie.compare(data[i], trie.reusePtr1, hash) == 0) c.accept(data[i])
                }
               return
            }

            if (!sorted) sortData(trie)
            var idx = binarySearch(trie, hash)
            if (idx < 0) return

            while (idx < dataCount && trie.compare(data[idx], trie.reusePtr1, hash) == 0) {
                c.accept(data[idx])
                idx++
            }
            return
        }

        override fun findValue(trie: MutableMemoryHashTrie, hash: ByteArray, comparator: IntUnaryOperator, removeOnMatch: Boolean): Int {
            if (dataCount <= 16) {
                for(i in 0 until dataCount) {
                    val testIdx = data[i]
                    if (trie.compare(testIdx, trie.reusePtr1, hash) == 0 && deletions?.contains(testIdx) != true && comparator.applyAsInt(testIdx) == 1) {
                        if (removeOnMatch) {
                            deletions = (deletions ?: RoaringBitmap()).apply { add(testIdx) }
                        }
                        return testIdx
                    }
                }
            } else {
                if (!sorted) sortData(trie)
                var idx = binarySearch(trie, hash)
                if (idx < 0) return -1
                while (idx < dataCount && trie.compare(data[idx], trie.reusePtr1, hash) == 0) {
                    val testIdx = data[idx]
                    if (deletions?.contains(testIdx) != true && comparator.applyAsInt(testIdx) == 1) {
                        if (removeOnMatch) {
                            deletions = (deletions ?: RoaringBitmap()).apply { add(testIdx) }
                        }
                        return testIdx
                    }
                    idx++
                }
            }
            return -1
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
