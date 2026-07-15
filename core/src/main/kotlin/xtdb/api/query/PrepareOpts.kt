package xtdb.api.query

import org.apache.arrow.vector.types.pojo.Field
import xtdb.database.DatabaseName
import java.time.Instant
import java.time.ZoneId

data class PrepareOpts @JvmOverloads constructor(
    val currentTime: Instant? = null,
    val defaultTz: ZoneId? = null,
    val defaultDb: DatabaseName? = null,

    // arg types for a parameterised DML/patch prepare — the planner keys its cache on them.
    val argFields: List<Field>? = null,

    val explain: Boolean = false,
    val explainAnalyze: Boolean = false,

    // the connection's resolved read basis at prepare time — gates the snapshot that prepare-time schema
    // (table-info) reads, so a freshly-committed table is visible when planning the statement that references it.
    val snapshotToken: String? = null,
)