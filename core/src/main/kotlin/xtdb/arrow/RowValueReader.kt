package xtdb.arrow

/**
 * A [ValueReader] that is also an iterator over its vector's rows: it starts before row 0, and `next()` moves it
 * to the following row and returns it.
 * A reader that can be [pos]itioned can be iterated, so a caller that seeks and a caller that scans share one reader.
 *
 * An iteration visits the rows present when the reader was made; a seek may reach rows appended since.
 */
interface RowValueReader : ValueReader, Iterator<RowValueReader> {
    override fun next(): RowValueReader {
        if (!hasNext()) throw NoSuchElementException()
        pos++
        return this
    }
}
