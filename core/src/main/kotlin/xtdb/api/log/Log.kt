@file:UseSerializers(DurationSerde::class, PathWithEnvVarSerde::class)
package xtdb.api.log

import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde

interface Log : TxLog, AutoCloseable {
    interface Factory {
        fun openLog(): Log
    }

    /**
     * @suppress
     */
    override fun close() {
    }
}
