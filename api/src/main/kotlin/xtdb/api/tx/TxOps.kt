package xtdb.api.tx

interface TxOp {
    data class Sql(@JvmField val sql: String, @JvmField val argRows: List<List<*>>? = null) : TxOp {
        fun argRows(argRows: List<List<*>>?): Sql = Sql(sql, argRows = argRows)
    }
}

object TxOps {
    @JvmStatic
    fun sql(sql: String) = TxOp.Sql(sql)
}
