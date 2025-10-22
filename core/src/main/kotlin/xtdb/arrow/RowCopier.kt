package xtdb.arrow

fun interface RowCopier {
    fun copyRow(sourceIdx: Int)

    fun copyRows(sel: IntArray) = sel.forEach { copyRow(it) }
    fun copyRange(startIdx: Int, len: Int) = repeat(len) { copyRow(startIdx + it) }
}
