package xtdb.query

import io.micrometer.tracing.Tracer
import org.apache.arrow.vector.types.pojo.Field
import xtdb.database.DatabaseName
import java.time.Instant
import java.time.ZoneId

data class QueryOpts @JvmOverloads constructor(
    val currentTime: Instant? = null,
    val defaultTz: ZoneId? = null,
    // the connection's resolved read basis — a tx's pinned begin-time token, else (autocommit) latest-completed
    // now. Gates the live snapshot so a read sees this connection's own writes, and is what SELECT SNAPSHOT_TOKEN
    // reports. Null only for callers that resolve none (they read whatever's cached).
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
    // arg types for a parameterised DML/patch prepare — the planner keys its cache on them.
    val argFields: List<Field>? = null,
    // explain a raw RA-plan prepare; for SQL the flag is read off the parse tree instead.
    val explain: Boolean = false,
    val explainAnalyze: Boolean = false,
    // the connection's resolved read basis at prepare time — gates the snapshot that prepare-time schema
    // (table-info) reads, so a freshly-committed table is visible when planning the statement that references it.
    val snapshotToken: String? = null,
)
