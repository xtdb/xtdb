package xtdb.util

class RowCounter {
    var blockRowCount: Long = 0
        private set

    fun nextBlock() {
        blockRowCount = 0
    }

    fun addRows(rowCount: Int) {
        blockRowCount += rowCount.toLong()
    }
}
