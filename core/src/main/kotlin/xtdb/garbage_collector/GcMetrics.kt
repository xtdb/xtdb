package xtdb.garbage_collector

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import xtdb.Meters
import xtdb.api.DatabaseName

/**
 * A database's garbage-collection meters. The collectors that record into them are built per leader term,
 * and these outlive any one term — so a node that has never led still reports them.
 */
class GcMetrics(registry: MeterRegistry?, private val dbName: DatabaseName) : AutoCloseable {

    private val meters = Meters(registry)

    private fun deleteTimer(name: String) = meters.register { reg ->
        Timer.builder(name)
            .publishPercentiles(0.75, 0.95, 0.99)
            .tag("db", dbName)
            .register(reg)
    }

    val trieDelete = deleteTimer("xtdb.gc.tries.delete.timer")
    val blockDelete = deleteTimer("xtdb.gc.block_files.delete.timer")
    val tableBlockDelete = deleteTimer("xtdb.gc.table_block_files.delete.timer")

    override fun close() = meters.close()
}
