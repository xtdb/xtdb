@file:UseSerializers(DurationSerde::class)

package xtdb.api

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import java.time.Duration

@Serializable
data class IndexerConfig(
    var logLimit: Long = 64L,
    var pageLimit: Long = 1024L,
    var rowsPerBlock: Long = 102400L,
    var flushDuration: Duration = Duration.ofHours(4),
) {
    fun logLimit(logLimit: Long) = apply { this.logLimit = logLimit }
    fun pageLimit(pageLimit: Long) = apply { this.pageLimit = pageLimit }
    fun rowsPerBlock(rowsPerBlock: Long) = apply { this.rowsPerBlock = rowsPerBlock }
    fun flushDuration(flushDuration: Duration) = apply { this.flushDuration = flushDuration }
}
