package xtdb.api

import java.time.Instant
import java.time.ZoneId

data class TxOptions(
    val systemTime: Instant? = null,
    val defaultTz: ZoneId? = null,
    val defaultAllValidTime: Boolean = false
)
