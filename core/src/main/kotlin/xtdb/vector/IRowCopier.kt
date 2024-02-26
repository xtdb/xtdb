package xtdb.vector

fun interface IRowCopier {
    fun copyRow(sourceIdx: Int): Int
}
