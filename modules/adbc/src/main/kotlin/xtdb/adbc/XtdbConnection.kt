package xtdb.adbc

import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcException
import org.apache.arrow.adbc.core.AdbcStatement
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import xtdb.api.Xtdb

class XtdbConnection(
    private val allocator: BufferAllocator,
    private val node: Xtdb
) : AdbcConnection {

    override fun createStatement(): AdbcStatement {
        return XtdbStatement(allocator, node)
    }

    override fun getInfo(infoCodes: IntArray?): ArrowReader {
        throw AdbcException.notImplemented("getInfo")
    }

    override fun close() {
        // Connection lifecycle is managed externally
    }
}
