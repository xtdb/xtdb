@file:UseSerializers(AnySerde::class, InstantSerde::class)

package xtdb.api.tx

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.AnySerde
import xtdb.InstantSerde

interface TxOp {
    @Serializable
    data class Sql(
        @JvmField @SerialName("sql") val sql: String,
        @JvmField val argRows: List<List<*>>? = null,
    ) : TxOp {

        fun argRows(argRows: List<List<*>>?): Sql = Sql(sql, argRows = argRows)
    }
}

object TxOps {
    @JvmStatic
    fun sql(sql: String) = TxOp.Sql(sql)
}
