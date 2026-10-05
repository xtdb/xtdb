package xtdb.api.query

import io.micrometer.tracing.Tracer
import xtdb.InternalApi
import java.time.Instant
import java.time.ZoneId

/** @suppress */
@InternalApi
data class QueryBasis(
    val currentTime: Instant,
    val defaultTz: ZoneId,
    val snapshotToken: String,
    val snapshotTime: Instant?,
) {
    fun toQueryOpts(tracer: Tracer?) = QueryOpts(currentTime, defaultTz, snapshotToken, snapshotTime, tracer)
}
