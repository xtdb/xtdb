package xtdb.debezium

import xtdb.api.TxId
import xtdb.debezium.proto.DebeziumOffsetToken
import xtdb.indexer.OpenTx
import java.time.Instant

class DebeziumMessage(
    val txId: TxId,
    val systemTime: Instant,
    val openTx: OpenTx,
    val offsets: DebeziumOffsetToken,
)
