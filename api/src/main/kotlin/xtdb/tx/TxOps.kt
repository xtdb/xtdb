package xtdb.tx

sealed interface ClientTxOp

data class Sql @JvmOverloads constructor(val sql: String, val argRows: List<List<Any?>>? = null) : ClientTxOp
data class SqlByteArgs(val sql: String, val argBytes: ByteArray) : ClientTxOp
data class PutDocs @JvmOverloads constructor(val tableName: Any?, val docs: List<Map<*, *>>, val validFrom: Any? = null, val validTo: Any? = null) : ClientTxOp
data class PutRel(val tableName: Any?, val relBytes: ByteArray) : ClientTxOp
data class PatchDocs @JvmOverloads constructor(val tableName: Any?, val docs: List<Map<*, *>>, val validFrom: Any? = null, val validTo: Any? = null) : ClientTxOp
data class DeleteDocs @JvmOverloads constructor(val tableName: Any?, val docIds: List<Any?>, val validFrom: Any? = null, val validTo: Any? = null) : ClientTxOp
data class EraseDocs(val tableName: Any?, val docIds: List<Any?>) : ClientTxOp
