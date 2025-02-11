@file:UseSerializers(DurationSerde::class)

package xtdb.api

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.log.LogOffset
import java.time.Duration

@Serializable
data class IndexerConfig(
    var logLimit: Long = 64L,
    var pageLimit: Long = 1024L,
    var rowsPerBlock: Long = 102400L,
    var flushDuration: Duration = Duration.ofHours(4),
    var skipTxs: List<LogOffset> = System.getenv("XTDB_SKIP_TXS")?.let(::parseSkipTxsEnv).orEmpty()
) {
    fun logLimit(logLimit: Long) = apply { this.logLimit = logLimit }
    fun pageLimit(pageLimit: Long) = apply { this.pageLimit = pageLimit }
    fun rowsPerBlock(rowsPerBlock: Long) = apply { this.rowsPerBlock = rowsPerBlock }
    fun flushDuration(flushDuration: Duration) = apply { this.flushDuration = flushDuration }
    fun skipTxs(skipTxs: List<LogOffset>) = apply { this.skipTxs = skipTxs.sorted() }

    companion object {
        private fun parseSkipTxsEnv(skipTxs: String): List<LogOffset> =
            skipTxs.split(",").map { it.trim().toLong() }.sorted()
    }
}

