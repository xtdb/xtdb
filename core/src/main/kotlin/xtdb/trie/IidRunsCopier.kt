package xtdb.trie

import xtdb.arrow.LongLongVectorReader
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.RowCopier
import xtdb.arrow.RunEndEncodedVector
import xtdb.arrow.VectorType

private const val IID = "_iid"
private val RUN_ENCODED_IID = VectorType.IID.runEndEncoded()

/**
 * Copies rows into a data relation whose `_iid` is run-end encoded, writing each iid straight from its two longs.
 *
 * A row extends `dest`'s own last run rather than one this copier remembers, so copiers from several inputs
 * interleaving into one destination coalesce, as do the rows of a selection that isn't contiguous.
 */
internal class IidRunsCopier(src: RelationReader, dest: Relation) : RowCopier {
    private val srcIid = src[IID] as LongLongVectorReader
    private val destIid = (dest[IID] as RunEndEncodedVector)
        .also { require(it.type == RUN_ENCODED_IID) { "can't write iids as runs into ${it.type}" } }
    private val others = src.rowCopierExcluding(dest, setOf(IID))

    private fun copyIid(idx: Int) = destIid.appendLongLong(srcIid.getLongLongHigh(idx), srcIid.getLongLongLow(idx))

    override fun copyRow(srcIdx: Int) {
        copyIid(srcIdx)
        others.copyRow(srcIdx)
    }

    override fun copyRows(sel: IntArray) {
        sel.forEach(::copyIid)
        others.copyRows(sel)
    }

    override fun copyRange(startIdx: Int, len: Int) {
        for (idx in startIdx..<startIdx + len) copyIid(idx)
        others.copyRange(startIdx, len)
    }
}

internal fun RelationReader.rowCopierFor(dest: Relation): RowCopier =
    if (dest[IID] is RunEndEncodedVector) IidRunsCopier(this, dest) else rowCopier(dest)
