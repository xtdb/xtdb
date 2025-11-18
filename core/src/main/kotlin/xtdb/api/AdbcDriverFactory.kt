package xtdb.api

import org.apache.arrow.adbc.core.AdbcDriver
import org.apache.arrow.adbc.core.TypedKey
import org.apache.arrow.adbc.drivermanager.AdbcDriverFactory
import org.apache.arrow.memory.BufferAllocator
import java.nio.file.Path

class AdbcDriverFactory : AdbcDriverFactory {
    companion object {
        val XTDB_CONFIG = TypedKey("xtdb.config", Xtdb.Config::class.java)
        val XTDB_CONFIG_YAML = TypedKey("xtdb.config-yaml", String::class.java)
        val XTDB_CONFIG_FILE = TypedKey("xtdb.config-path", Path::class.java)

        fun getDriver(allocator: BufferAllocator) = AdbcDriver { opts ->
            // TODO use this allocator rather than creating our own
            Xtdb.openNode(
                XTDB_CONFIG[opts]
                    ?: XTDB_CONFIG_YAML[opts]?.let { nodeConfig(it) }
                    ?: XTDB_CONFIG_FILE[opts]?.let { Xtdb.readConfig(it) }
                    ?: Xtdb.Config())
        }
    }

    override fun getDriver(allocator: BufferAllocator) = Companion.getDriver(allocator)
}