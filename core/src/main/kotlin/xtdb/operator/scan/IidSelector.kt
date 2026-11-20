package xtdb.operator.scan

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.RelationReader
import xtdb.arrow.RowMask

sealed interface IidSelector {
    fun rowMask(al: BufferAllocator, page: RelationReader, path: ByteArray): RowMask
}
