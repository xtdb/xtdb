@file:UseSerializers(InstantSerde::class, ZoneIdSerde::class)
package xtdb.http

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.InstantSerde
import xtdb.ZoneIdSerde
import java.time.Instant
import java.time.ZoneId

@Serializable
data class TxOptions(
    val systemTime: Instant? = null,
    val defaultTz: ZoneId? = null,
    val user: String? = null
)
