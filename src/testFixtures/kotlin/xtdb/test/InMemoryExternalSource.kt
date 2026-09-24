package xtdb.test

import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.dropWhile
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import xtdb.api.tx.ExternalSource
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.tx.OpenTx
import xtdb.api.tx.TxIndexer
import xtdb.api.tx.TxIndexer.TxResult
import java.nio.ByteBuffer

/**
 * An upstream fed by hand: one replayable stream per partition, read by the [ExternalSource]s [open]ed over it.
 *
 * [publish] a message to a partition, and a term leading that partition turns it into a transaction.
 * Await it through that partition's `Watchers`.
 *
 * Each stream keeps every message, and a term reads from just after the token it resumes from, so a term that
 * dies mid-message re-reads it, and every source opened over this upstream sees every message.
 */
class InMemoryExternalSource(
    partitions: Int = 1,

    /**
     * How a message becomes a transaction — by default the blocking [TxIndexer.executeTx]; pass a
     * `submitTx`-based one to drive the fire-and-forget path.
     */
    private val index: suspend TxIndexer.(Msg) -> Unit = { executeTx(it.token, writer = it.writer) },
) {

    /** One upstream event: its resume marker, and what its transaction writes. */
    class Msg(val token: ExternalSourceToken, val writer: suspend (OpenTx) -> TxResult)

    private class Stream {
        val mutex = Mutex()
        var nextOffset = 0L

        // unbounded: a term replays from wherever it resumes, and a capped buffer would drop the oldest
        // messages while nothing is reading, which is when tests publish
        val msgs = MutableSharedFlow<Pair<Long, Msg>>(replay = Int.MAX_VALUE)
    }

    private val streams = List(partitions) { Stream() }

    /** @return the message's resume marker, as the database will persist it with the transaction. */
    suspend fun publish(
        partition: Int = 0,
        writer: suspend (OpenTx) -> TxResult = { TxResult.Committed() },
    ): ExternalSourceToken {
        val stream = streams[partition]

        // offsets are assigned and emitted under one lock, so every reader sees them in order
        return stream.mutex.withLock {
            val offset = stream.nextOffset++
            val token = offset.toToken()
            stream.msgs.emit(offset to Msg(token, writer))
            token
        }
    }

    private inner class Source : ExternalSource {
        override suspend fun onPartitionAssigned(
            partition: Int, afterToken: ExternalSourceToken?, txIndexer: TxIndexer
        ) {
            val after = afterToken?.toOffset() ?: -1

            streams[partition].msgs
                .dropWhile { (offset, _) -> offset <= after }
                .collect { (_, msg) -> txIndexer.index(msg) }
        }

        // the upstream outlives every source opened over it
        override fun close() = Unit
    }

    /** A source over this upstream, as a database opens one: called for each partition this node leads. */
    fun open(): ExternalSource = Source()

    private companion object {
        fun Long.toToken(): ExternalSourceToken = ByteBuffer.allocate(Long.SIZE_BYTES).putLong(this).array()
        fun ExternalSourceToken.toOffset() = ByteBuffer.wrap(this).long
    }
}
