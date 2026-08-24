package xtdb.indexer

import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import xtdb.Meters
import xtdb.api.DatabaseName

/**
 * A database's follower-side meters. The node opens a fresh [FollowerLogProcessor] every time it returns
 * to following, and these outlive any one of them.
 */
class ReplicaMetrics(registry: MeterRegistry?, private val dbName: DatabaseName) : AutoCloseable {

    private val meters = Meters(registry)

    private fun processTimer(msgType: String) = meters.register { reg ->
        Timer.builder("xtdb.replica.process.timer")
            .description("Time spent processing replica log records, by message type")
            .tag("db", dbName)
            .tag("msg.type", msgType)
            .publishPercentiles(0.75, 0.85, 0.95, 0.98, 0.99, 0.999)
            .register(reg)
    }

    val resolvedTx = processTimer("ResolvedTx")
    val triesAdded = processTimer("TriesAdded")
    val blockBoundary = processTimer("BlockBoundary")
    val blockUploaded = processTimer("BlockUploaded")
    val triesDeleted = processTimer("TriesDeleted")

    val blockBuffer = meters.register { reg ->
        Timer.builder("xtdb.replica.block.buffer.timer")
            .description("Time the follower spends buffering records between BlockBoundary and BlockUploaded")
            .tag("db", dbName)
            .publishPercentiles(0.75, 0.85, 0.95, 0.98, 0.99, 0.999)
            .register(reg)
    }

    val bufferedRecords = meters.register { reg ->
        DistributionSummary.builder("xtdb.replica.block.buffered.records")
            .description("Number of records buffered while a follower waits for BlockUploaded")
            .tag("db", dbName)
            .register(reg)
    }

    override fun close() = meters.close()
}
