package xtdb.arrow

fun interface RowCopier {
    fun copyRow(sourceIdx: Int): Int
}
