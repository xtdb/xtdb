package xtdb.query

import io.micrometer.tracing.Tracer
import xtdb.database.DatabaseName
import java.time.Instant
import java.time.ZoneId

data class QueryOpts @JvmOverloads constructor(
    val currentTime: Instant? = null,
    val defaultTz: ZoneId? = null,
    val snapshotToken: String? = null,
    val snapshotTime: Instant? = null,
    val tracer: Tracer? = null,
)

/**
 * Prepare-time options for [IQuerySource.prepareQuery]. The query planner consumes these as a Clojure map
 * today; this typed form is the Kotlin-facing boundary, converted to that map at the planner's entry. The
 * conversion is being pushed one layer deeper at a time, shrinking the map's reach as it goes.
 */
data class PrepareOpts @JvmOverloads constructor(
    val defaultTz: ZoneId? = null,
    val defaultDb: DatabaseName? = null,
    val currentTime: Instant? = null,
)
