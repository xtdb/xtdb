package xtdb.arrow.agg

fun interface VectorSummer {
    // we can optimize this further later à la RowCopier
    fun sumRow(idx: Int, groupIdx: Int)
}