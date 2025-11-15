package xtdb.adbc

import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcDatabase
import org.apache.arrow.adbc.core.AdbcException
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.Xtdb

class XtdbDatabase(
    private val allocator: BufferAllocator,
    private val node: Xtdb
) : AdbcDatabase {

    override fun connect(): AdbcConnection {
        return XtdbConnection(allocator, node)
    }

    override fun close() {
        // Database lifecycle is managed externally
        // The node should not be closed by the ADBC driver
    }
}
