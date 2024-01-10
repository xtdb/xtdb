package xtdb.api.tx

import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import java.time.Instant
import java.time.ZoneId

@Serializable
data class TxOptions(
    @Contextual val systemTime: Instant? = null,
    @Contextual val defaultTz: ZoneId? = null,
    val defaultAllValidTime: Boolean = false
) {
    companion object {
        @JvmStatic
        fun txOpts() = Builder()
    }

    class Builder {
        private var systemTime: Instant? = null
        private var defaultTz: ZoneId? = null
        private var defaultAllValidTime = false

        fun systemTime(systemTime: Instant?) = apply { this.systemTime = systemTime }
        fun defaultTz(defaultTz: ZoneId?) = apply { this.defaultTz = defaultTz }
        fun defaultAllValidTime(defaultAllValidTime: Boolean) = apply { this.defaultAllValidTime = defaultAllValidTime }
        fun build() = TxOptions(systemTime, defaultTz, defaultAllValidTime)
    }
}
