@file:JvmName("Basis")

package xtdb.database

import xtdb.api.log.MessageId
import xtdb.error.Incorrect
import xtdb.proto.basis.TxBasis
import xtdb.proto.basis.messageIds
import xtdb.proto.basis.txBasis
import java.util.function.BiFunction
import kotlin.io.encoding.Base64
import kotlin.math.max

typealias TxBasisToken = Map<DatabaseName, List<MessageId>>

fun TxBasisToken.encodeTxBasisToken(): String =
    txBasis {
        entries.forEach {
            databases.put(it.key, messageIds { messageIds.addAll(it.value) })
        }
    }
        .toByteArray()
        .let { Base64.encode(it) }

fun String.decodeTxBasisToken(): TxBasisToken =
    runCatching { TxBasis.parseFrom(Base64.decode(this)) }
        .getOrElse {
            throw Incorrect(
                message = "Invalid basis: ${it.message}",
                errorCode = "xtdb.basis/invalid-basis",
                cause = it
            )
        }
        .let { basis ->
            basis.databasesMap.entries
                .associate { it.key to it.value.messageIdsList }
        }

fun mergeTxBasisTokens(l: String?, r: String): String {
    if (l == null) return r

    return l.decodeTxBasisToken()
        .toMutableMap()
        .also { res ->
            for ((dbName, msgIds) in r.decodeTxBasisToken()) {
                res.merge(dbName, msgIds) { ls, rs ->
                    when {
                        ls.size != rs.size -> error("merging bases with different number of partitions")
                        else -> ls.zip(rs) { l, r -> max(l, r) }
                    }
                }
            }
        }
        .encodeTxBasisToken()
}