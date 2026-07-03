@file:JvmName("Basis")

package xtdb.database

import com.google.protobuf.Timestamp
import xtdb.api.log.MessageId
import xtdb.error.Incorrect
import xtdb.proto.basis.*
import java.time.Instant
import kotlin.io.encoding.Base64
import kotlin.math.max

typealias DatabaseName = String
typealias TxBasisToken = Map<DatabaseName, List<MessageId>>

// a read snapshot's upper bound: per database, the latest-completed system-time of each partition
// (null where a partition has completed no transactions).
typealias TimeBasisToken = Map<DatabaseName, List<Instant?>>

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

fun TimeBasisToken.encodeTimeBasisToken(): String =
    timeBasis {
        entries.forEach { (dbName, sysTimes) ->
            databases.put(dbName, systemTimes {
                systemTimes.addAll(sysTimes.map {
                    it?.let { Timestamp.newBuilder().setSeconds(it.epochSecond).setNanos(it.nano).build() }
                        ?: Timestamp.getDefaultInstance()
                })
            })
        }
    }
        .toByteArray()
        .let { Base64.encode(it) }

fun String.decodeTimeBasisToken(): TimeBasisToken =
    runCatching { TimeBasis.parseFrom(Base64.decode(this)) }
        .getOrElse {
            throw Incorrect(
                message = "Invalid basis: ${it.message}",
                errorCode = "xtdb.basis/invalid-basis",
                cause = it
            )
        }
        .let { basis ->
            basis.databasesMap.entries
                .associate { (dbName, sysTimes) ->
                    dbName to sysTimes.systemTimesList.map {
                        if (it == Timestamp.getDefaultInstance()) null
                        else Instant.ofEpochSecond(it.seconds, it.nanos.toLong())
                    }
                }
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