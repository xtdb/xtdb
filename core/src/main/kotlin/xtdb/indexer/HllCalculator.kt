package xtdb.indexer

import xtdb.arrow.VectorReader
import xtdb.trie.ColumnName
import xtdb.util.HLL
import xtdb.util.add
import xtdb.util.createHLL
import xtdb.vector.IVectorReader

class HllCalculator {

    private val hlls = mutableMapOf<ColumnName, HLL>()

    fun update(putRdr: IVectorReader, startPos: Int, endPos: Int) {
        val columns = putRdr.structKeys()
        val keyRdrs = columns.map { it to VectorReader.from(putRdr.structKeyReader(it)!!) }

        for ((col, rdr) in keyRdrs) {
            hlls.compute(col) { _, hll ->
                (hll ?: createHLL()).also {
                    for (i in startPos..<endPos)
                        it.add(rdr, i)
                }
            }
        }
    }

    fun build(): Map<ColumnName, HLL> = hlls
}