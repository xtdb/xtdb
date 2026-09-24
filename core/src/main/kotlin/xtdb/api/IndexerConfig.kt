@file:UseSerializers(DurationSerde::class)

package xtdb.api

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.types.MessageId
import java.time.Duration

@Serializable
data class IndexerConfig(
    var logLimit: Long = 64L,
    var pageLimit: Long = 1024L,
    var rowsPerBlock: Long = 102400L,
    var flushDuration: Duration = Duration.ofMinutes(15),
    var skipTxs: List<MessageId> = System.getenv("XTDB_SKIP_TXS")?.let(::parseSkipTxsEnv).orEmpty(),
    var enabled: Boolean = true,
    /**
     * Whether a leader sends each replica-log record without waiting for the one before it to be durable.
     *
     * Temporary: pipelining becomes unconditional in the next release, and this goes with it.
     * Enable it only once every node of the database runs this release — an older node would apply
     * straight through a record lost in flight.
     * Defaults to whether [PIPELINED_REPLICA_APPENDS_ENV] is set, which also lets the Kafka producer linger.
     */
    var pipelinedReplicaAppends: Boolean = System.getenv(PIPELINED_REPLICA_APPENDS_ENV) != null,
) {
    fun logLimit(logLimit: Long) = apply { this.logLimit = logLimit }
    fun pageLimit(pageLimit: Long) = apply { this.pageLimit = pageLimit }
    fun rowsPerBlock(rowsPerBlock: Long) = apply { this.rowsPerBlock = rowsPerBlock }
    fun flushDuration(flushDuration: Duration) = apply { this.flushDuration = flushDuration }
    fun skipTxs(skipTxs: List<MessageId>) = apply { this.skipTxs = skipTxs.sorted() }
    fun enabled(enabled: Boolean) = apply { this.enabled = enabled }
    fun pipelinedReplicaAppends(pipelined: Boolean) = apply { this.pipelinedReplicaAppends = pipelined }

    companion object {
        const val PIPELINED_REPLICA_APPENDS_ENV = "XTDB_PIPELINED_REPLICA_APPENDS"

        private fun parseSkipTxsEnv(skipTxs: String): List<MessageId> =
            skipTxs.split(",").map { it.trim().toLong() }.sorted()
    }
}

