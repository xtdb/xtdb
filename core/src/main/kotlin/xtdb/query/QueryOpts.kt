package xtdb.query

import io.micrometer.tracing.Tracer
import java.time.Instant
import java.time.ZoneId

data class QueryOpts @JvmOverloads constructor(
    val currentTime: Instant? = null,
    val defaultTz: ZoneId? = null,
    val snapshotToken: String? = null,
    val snapshotTime: Instant? = null,
    val tracer: Tracer? = null,
)
