@file:UseSerializers(InstantSerde::class)
package xtdb.http

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.InstantSerde
import xtdb.api.TransactionKey
import java.time.Instant

@Serializable
data class Basis(
    val atTx: TransactionKey? = null,

    val currentTime: Instant? = null
)
