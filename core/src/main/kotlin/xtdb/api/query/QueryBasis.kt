package xtdb.api.query

import io.micrometer.tracing.Tracer
import kotlinx.serialization.Serializable
import xtdb.InstantSerde
import xtdb.InternalApi
import xtdb.ZoneIdSerde
import java.time.Instant
import java.time.ZoneId

/** @suppress */
// public only because `Xtdb.Statement.queryBasis` returns it
@InternalApi
@Serializable
data class QueryBasis(
    @Serializable(InstantSerde::class) val currentTime: Instant,
    @Serializable(ZoneIdSerde::class) val defaultTz: ZoneId,
    val snapshotToken: String,
    @Serializable(InstantSerde::class) val snapshotTime: Instant?,
) {
    fun toQueryOpts(tracer: Tracer?) = QueryOpts(currentTime, defaultTz, snapshotToken, snapshotTime, tracer)
}
