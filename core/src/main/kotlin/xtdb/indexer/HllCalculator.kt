package xtdb.indexer

import xtdb.arrow.VectorReader
import xtdb.trie.ColumnName
import xtdb.util.HLL
import xtdb.util.add
import xtdb.util.createHLL

class HllCalculator {

    private val hlls = mutableMapOf<ColumnName, HLL>()

    fun update(opRdr: VectorReader, startPos: Int, endPos: Int) {
        val putRdr = opRdr.vectorFor("put")
        val columns = putRdr.keyNames.orEmpty()
        val keyRdrs = columns.map { it to putRdr.vectorFor(it) }

        for ((col, rdr) in keyRdrs) {
            hlls.compute(col) { _, hll ->
                (hll ?: createHLL()).also {
                    for (i in startPos..<endPos)
                        if (opRdr.getLeg(i) == "put")
                            it.add(rdr, i)
                }
            }
        }
    }

    fun build(): Map<ColumnName, HLL> = hlls
}
