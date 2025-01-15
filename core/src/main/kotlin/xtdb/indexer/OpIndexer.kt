package xtdb.indexer

import org.apache.arrow.vector.complex.DenseUnionVector

interface OpIndexer {
    /**
     * Returns a tx-ops-vec of more operations (mostly for `:call`).
     */
    fun indexOp(txOpIdx: Long): DenseUnionVector
}