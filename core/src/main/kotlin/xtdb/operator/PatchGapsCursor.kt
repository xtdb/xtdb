package xtdb.operator

import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.ICursor
import xtdb.arrow.RowCopier
import xtdb.vector.IRelationWriter
import xtdb.vector.RelationReader
import java.util.function.Consumer
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG

class PatchGapsCursor(
    private val inner: ICursor<RelationReader>,
    private val out: IRelationWriter,
    private val validFrom: Long,
    private val validTo: Long,
) : ICursor<RelationReader> {

    private val iidWriter = out.colWriter("_iid")
    private val vfWriter = out.colWriter("_valid_from")
    private val vtWriter = out.colWriter("_valid_to")
    private val docWriter = out.colWriter("doc")

    private fun copyRow(idx: Int, iidCopier: RowCopier, validFrom: Long, validTo: Long, docCopier: RowCopier?) {
        out.startRow()
        iidCopier.copyRow(idx)
        vfWriter.writeLong(validFrom)
        if (validTo == MAX_LONG) vtWriter.writeNull() else vtWriter.writeLong(validTo)
        docCopier?.copyRow(idx)
        out.endRow()
    }

    override fun tryAdvance(c: Consumer<in RelationReader>) = inner.tryAdvance { inRel ->
        out.clear()

        val iidReader = inRel.readerForName("_iid")
        val iidCopier = iidReader.rowCopier(iidWriter)
        val vfReader = inRel.readerForName("_valid_from")
        val vtReader = inRel.readerForName("_valid_to")
        val docCopier = inRel.readerForName("doc").rowCopier(docWriter)

        val currentIid = ArrowBufPointer()
        val prevIid = ArrowBufPointer()

        val rowCount = inRel.rowCount()
        if (rowCount > 0) {
            var currentValidTime = validFrom

            for (idx in 0 until rowCount) {
                iidReader.getPointer(idx, currentIid)
                if (currentIid != prevIid) {
                    if (idx > 0 && currentValidTime < validTo) {
                        copyRow(idx - 1, iidCopier, currentValidTime, validTo, null)
                    }

                    currentValidTime = validFrom
                    prevIid.set(currentIid.buf, currentIid.offset, currentIid.length)
                }

                val vf = vfReader.getLong(idx)
                val vt = if(vtReader.isNull(idx)) MAX_LONG else vtReader.getLong(idx)

                if (vf > currentValidTime) {
                    copyRow(idx, iidCopier, currentValidTime, vf.coerceAtMost(validTo), null)
                }

                copyRow(idx, iidCopier, vf.coerceAtLeast(validFrom), vt.coerceAtMost(validTo), docCopier)

                currentValidTime = vt
            }

            if (currentValidTime < validTo) {
                copyRow(rowCount - 1, iidCopier, currentValidTime, validTo, null)
            }
        }

        c.accept(out.toReader())
    }

    override fun close() {
        out.close()
        inner.close()
    }
}