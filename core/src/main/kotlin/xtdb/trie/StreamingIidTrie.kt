package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.RelationWriter
import xtdb.arrow.VectorReader
import xtdb.util.closeOnCatch

internal class RowBuffers(
    private val al: BufferAllocator, private val schema: Schema,
    private val input: RelationReader, private val output: RelationWriter
) : AutoCloseable {

    inner class Buffer(private val rel: Relation) : AutoCloseable {
        private val fromInput = input.rowCopierWithReeIid(rel)
        private val toOutput = rel.rowCopierWithReeIid(output)
        private val recencies = rel[Trie.RECENCY_COL]

        val iids: VectorReader = rel["_iid"]
        val rowCount get() = rel.rowCount

        fun add(inputIdx: Int) = fromInput.copyRow(inputIdx)

        fun writeOut(startIdx: Int, len: Int) = toOutput.copyRange(startIdx, len)

        fun maxRecency(startIdx: Int, len: Int): RecencyMicros =
            (startIdx until startIdx + len).maxOf { recencies.getLong(it) }

        fun clear() = rel.clear()

        fun moveTo(dest: Buffer, startIdx: Int) {
            rel.rowCopierWithReeIid(dest.rel).copyRange(startIdx, rowCount - startIdx)
            rel.clear()
        }

        override fun close() = rel.close()
    }

    fun open() = Relation(al, schema).closeOnCatch { Buffer(it) }

    private var spare = open()

    /**
     * @return a buffer holding [buf]'s rows from [startIdx]; the caller MUST NOT use [buf] again
     */
    fun dropHead(buf: Buffer, startIdx: Int): Buffer {
        val kept = spare
        buf.moveTo(kept, startIdx)
        spare = buf
        return kept
    }

    override fun close() = spare.close()
}

/**
 * Writes the count-driven IID trie of rows that arrive in IID order - the trie that splits any node of more than a page -
 * holding at most a page of them, plus one row.
 */
internal class StreamingIidTrie(
    private val buffers: RowBuffers,
    private val metaFileWriter: MetadataFileWriter,
    private val dataFileWriter: DataFileWriter,
    private val startDepth: Int,
    private val pageSize: Int,
) : AutoCloseable {

    private class Level(val depth: Int, var cur: Int) {
        val children = IntArray(DEFAULT_LEVEL_WIDTH) { -1 }
    }

    private sealed interface Open {
        data object Rows : Open

        class Entity(val pages: MutableList<Pair<RecencyMicros, RowIndex>>) : Open
    }

    private val spine = ArrayList<Level>()
    private var open: Open = Open.Rows
    private var buf = buffers.open()

    private val inputPtr = ArrowBufPointer()
    private val firstPtr = ArrowBufPointer()
    private val lastPtr = ArrowBufPointer()

    private val nextDepth get() = spine.lastOrNull()?.let { it.depth + 1 } ?: startDepth

    private fun bucket(iid: ArrowBufPointer, depth: Int) = Bucketer.DEFAULT.bucketFor(iid, depth).toInt()

    private fun writeLeaf(startIdx: Int, len: Int): RowIndex {
        buf.writeOut(startIdx, len)
        return metaFileWriter.writeLeaf().also { dataFileWriter.writePage() }
    }

    private fun Open.Entity.writePage(len: Int) {
        pages += buf.maxRecency(0, len) to writeLeaf(0, len)
        buf = buffers.dropHead(buf, len)
    }

    private fun closeOpen(): RowIndex =
        when (val open = open) {
            Open.Rows -> writeLeaf(0, buf.rowCount).also { buf.clear() }

            is Open.Entity -> {
                open.writePage(buf.rowCount)
                this.open = Open.Rows
                metaFileWriter.writeRecencyBranch(open.pages.sortedBy { it.first })
            }
        }

    private fun popTo(size: Int) {
        while (spine.size > size) {
            val level = spine.removeLast()
            spine.last().let { it.children[it.cur] = metaFileWriter.writeIidBranch(level.children) }
        }
    }

    private fun separateEntity(iid: ArrowBufPointer) {
        val entityIid = buf.iids.getPointer(0, firstPtr)

        var depth = nextDepth
        while (bucket(entityIid, depth) == bucket(iid, depth)) {
            spine += Level(depth, bucket(iid, depth))
            depth++
        }

        val entityBucket = bucket(entityIid, depth)
        val level = Level(depth, bucket(iid, depth))
        level.children[entityBucket] = closeOpen()
        spine += level
    }

    private fun split() {
        when (val open = open) {
            is Open.Entity -> open.writePage(pageSize)

            Open.Rows -> {
                val lastIid = buf.iids.getPointer(buf.rowCount - 1, lastPtr)

                if (buf.iids.getPointer(0, firstPtr) == lastIid) {
                    Open.Entity(mutableListOf()).also { this.open = it }.writePage(pageSize)
                    return
                }

                val depth = nextDepth
                val level = Level(depth, bucket(lastIid, depth))

                // all but the last bucket are complete, and hold at most a page between them
                var start = 0
                while (true) {
                    val bucket = bucket(buf.iids.getPointer(start, firstPtr), depth)
                    if (bucket == level.cur) break

                    var end = start + 1
                    while (bucket(buf.iids.getPointer(end, firstPtr), depth) == bucket) end++

                    level.children[bucket] = writeLeaf(start, end - start)
                    start = end
                }

                spine += level
                buf = buffers.dropHead(buf, start)
            }
        }
    }

    /**
     * @param inputIdx a row of the [RowBuffers]' input, with an IID no lower than any row added before it
     */
    fun add(inputIids: VectorReader, inputIdx: Int) {
        val iid = inputIids.getPointer(inputIdx, inputPtr)
        val divergedAt = spine.indexOfFirst { bucket(iid, it.depth) != it.cur }

        if (divergedAt >= 0) {
            spine.last().let { it.children[it.cur] = closeOpen() }
            popTo(divergedAt + 1)
            spine[divergedAt].cur = bucket(iid, spine[divergedAt].depth)
        } else if (open is Open.Entity && buf.iids.getPointer(0, firstPtr) != iid) {
            separateEntity(iid)
        }

        buf.add(inputIdx)
        while (buf.rowCount > pageSize) split()
    }

    /**
     * Writes what's left of the trie; call once, after adding at least one row.
     *
     * @return the trie's root
     */
    fun end(): RowIndex {
        val subtree = closeOpen()
        if (spine.isEmpty()) return subtree

        spine.last().let { it.children[it.cur] = subtree }
        popTo(1)
        return metaFileWriter.writeIidBranch(spine.single().children)
    }

    override fun close() = buf.close()
}
