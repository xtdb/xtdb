package xtdb.adbc

import org.apache.arrow.adbc.core.AdbcDatabase
import org.apache.arrow.adbc.core.AdbcDriver
import org.apache.arrow.adbc.core.AdbcException
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.Xtdb

class XtdbDriver(private val allocator: BufferAllocator) : AdbcDriver {

    companion object {
        const val PARAM_XTDB_NODE = "xtdb.node"
    }

    override fun open(parameters: MutableMap<String, Any>?): AdbcDatabase {
        val node = parameters?.get(PARAM_XTDB_NODE) as? Xtdb
            ?: throw IllegalArgumentException("Missing required parameter: $PARAM_XTDB_NODE")

        return XtdbDatabase(allocator, node)
    }
}
