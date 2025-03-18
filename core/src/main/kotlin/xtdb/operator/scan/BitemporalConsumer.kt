package xtdb.operator.scan

import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.TEMPORAL_COL_TYPE
import xtdb.bitemporal.IRowConsumer
import xtdb.trie.ColumnName
import xtdb.vector.IRelationWriter
import xtdb.vector.IVectorWriter
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG

class BitemporalConsumer(outRel: IRelationWriter, colNames: Set<ColumnName>) : IRowConsumer {

    private val validFromVec: IVectorWriter?
    private val validToVec: IVectorWriter?
    private val systemFromVec: IVectorWriter?
    private val systemToVec: IVectorWriter?

    init {
        fun writerFor(colName: ColumnName, nullable: Boolean) =
            if (colName in colNames)
                outRel.colWriter(colName, FieldType(nullable, TEMPORAL_COL_TYPE, null))
            else null

        validFromVec = writerFor("_valid_from", false)
        validToVec = writerFor("_valid_to", true)
        systemFromVec = writerFor("_system_from", false)
        systemToVec = writerFor("_system_to", true)
    }


    override fun accept(validFrom: Long, validTo: Long, systemFrom: Long, systemTo: Long) {
        validFromVec?.writeLong(validFrom)

        if (validToVec != null) {
            if (validTo == MAX_LONG) validToVec.writeNull() else validToVec.writeLong(validTo)
        }

        systemFromVec?.writeLong(systemFrom)

        if (systemToVec != null) {
            if (systemTo == MAX_LONG) systemToVec.writeNull() else systemToVec.writeLong(systemTo)
        }
    }
}