package xtdb.trie

import xtdb.arrow.LongLongVectorReader
import xtdb.arrow.RelationReader
import xtdb.arrow.RelationWriter
import xtdb.arrow.RowCopier
import xtdb.arrow.RunEndEncodedVector
import xtdb.arrow.VectorType
import xtdb.arrow.VectorWriter

private const val IID = "_iid"
private val RUN_ENCODED_IID = VectorType.IID.runEndEncoded()

/**
 * Copies iids into a run-end encoded `_iid`, writing each one straight from its two longs.
 *
 * A row extends `dest`'s own last run rather than one this copier remembers, so copiers from several inputs
 * interleaving into one destination coalesce, as do the rows of a selection that isn't contiguous.
 */
private class IidRunsCopier(private val src: LongLongVectorReader, private val dest: VectorWriter) : RowCopier {
    override fun copyRow(srcIdx: Int) = dest.writeLongLong(src.getLongLongHigh(srcIdx), src.getLongLongLow(srcIdx))
}

internal fun RelationReader.rowCopierWithReeIid(dest: RelationWriter): RowCopier {
    val destIid = dest[IID] as? RunEndEncodedVector ?: return rowCopier(dest)
    require(destIid.type == RUN_ENCODED_IID) { "can't write iids as runs into ${destIid.type}" }

    val copiers = dest.vectors.map { destVec ->
        if (destVec === destIid) IidRunsCopier(this[IID] as LongLongVectorReader, destIid)
        else this[destVec.name].rowCopier(destVec)
    }

    return object : RowCopier {
        override fun copyRow(srcIdx: Int) {
            copiers.forEach { it.copyRow(srcIdx) }
            dest.rowCount++
        }

        override fun copyRows(sel: IntArray) {
            copiers.forEach { it.copyRows(sel) }
            dest.rowCount += sel.size
        }

        override fun copyRange(startIdx: Int, len: Int) {
            copiers.forEach { it.copyRange(startIdx, len) }
            dest.rowCount += len
        }
    }
}
