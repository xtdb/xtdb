package xtdb.arrow

/**
 * A vector's reader whose `isNull` reads its [validity] 64 rows at a time, as one `Long`.
 *
 * Reads through the [BitBuffer] rather than its `ArrowBuf`, which a write that grows the vector frees.
 * A seek reloads the word, as the vector may have been written to since the last one; `next()` reuses it.
 */
internal abstract class ValidityWordRows(private val rowCount: Int) : RowValueReader {

    protected abstract val validity: BitBuffer?

    private var wordIdx = -1
    private var word = 0L

    private fun loadWord(validity: BitBuffer, wordIdx: Int) {
        val buf = validity.buf
        val offset = wordIdx.toLong() * Long.SIZE_BYTES
        val capacity = buf.capacity()

        word = if (offset + Long.SIZE_BYTES <= capacity) buf.getLong(offset) else {
            var acc = 0L
            var byteIdx = 0
            while (offset + byteIdx < capacity) {
                acc = acc or ((buf.getByte(offset + byteIdx).toLong() and 0xff) shl (byteIdx * Byte.SIZE_BITS))
                byteIdx++
            }
            acc
        }

        this.wordIdx = wordIdx
    }

    private var row = -1

    /** Called on every seek, and not on `next()`. */
    protected open fun onSeek() {}

    final override var pos: Int
        get() = row
        set(value) {
            row = value
            validity?.let { loadWord(it, value shr 6) }
            onSeek()
        }

    final override fun hasNext() = row + 1 < rowCount

    final override fun next(): RowValueReader {
        if (!hasNext()) throw NoSuchElementException()
        val row = ++row

        if (row shr 6 != wordIdx) validity?.let { loadWord(it, row shr 6) }

        return this
    }

    final override val isNull get() = validity != null && (word ushr (row and 63)) and 1L == 0L
}
