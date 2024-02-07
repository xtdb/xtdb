package xtdb.util

class RowCounter(var chunkIdx: Long) {
    var chunkRowCount: Long = 0
        private set

    fun nextChunk() {
        chunkIdx += chunkRowCount
        chunkRowCount = 0
    }

    fun addRows(rowCount: Int) {
        chunkRowCount += rowCount.toLong()
    }
}
