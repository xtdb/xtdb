package xtdb.indexer

import xtdb.database.Database
import xtdb.database.Database.Mode.*

class LogProcessor(private val factory: SystemFactory, initialMode: Database.Mode) : AutoCloseable {

    interface System : AutoCloseable

    interface SystemFactory {
        fun openReadWriteSystem(): System
        fun openReadOnlySystem(): System
    }

    private fun openSystem(mode: Database.Mode) = when (mode) {
        READ_WRITE -> factory.openReadWriteSystem()
        READ_ONLY -> factory.openReadOnlySystem()
    }

    @Volatile
    private var system: System = openSystem(initialMode)

    override fun close() {
        system.close()
    }
}