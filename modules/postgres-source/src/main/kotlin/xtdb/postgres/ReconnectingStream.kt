package xtdb.postgres

import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import org.postgresql.replication.LogSequenceNumber
import org.postgresql.util.PSQLException
import xtdb.util.logger
import xtdb.util.warn
import kotlin.time.TimeMark
import kotlin.time.TimeSource

private val LOG = ReconnectingStream::class.logger

/**
 * A [PostgresDriver.ChangeStream] that reopens its underlying stream whenever the replication connection
 * fails, however many times it takes, presenting each transaction at most once.
 *
 * Only a [PSQLException] from opening, polling or acknowledging counts as a failure; anything else, and
 * any failure once the calling coroutine is cancelled, propagates.
 *
 * Owns the stream it holds: closing this closes that one. A reopen resumes from the last transaction
 * presented, so nothing the caller has already seen arrives twice.
 *
 * @suppress
 */
class ReconnectingStream private constructor(
    private val dbName: String,
    private val opener: suspend (Long) -> PostgresDriver.ChangeStream,
    private val policy: ReconnectPolicy,
    private val timeSource: TimeSource,
    startLsn: Long,
) : PostgresDriver.ChangeStream {

    companion object {
        suspend fun open(
            dbName: String,
            opener: suspend (Long) -> PostgresDriver.ChangeStream,
            startLsn: Long,
            policy: ReconnectPolicy = ReconnectPolicy(),
            timeSource: TimeSource = TimeSource.Monotonic,
        ) = ReconnectingStream(dbName, opener, policy, timeSource, startLsn).also { it.ensureConnected() }
    }

    private sealed interface Conn {
        data object Down : Conn
        class Open(val stream: PostgresDriver.ChangeStream, val openedAt: TimeMark) : Conn
    }

    @Volatile private var conn: Conn = Conn.Down

    private var presentedLsn = startLsn

    private var failures = 0

    private suspend fun <T> withStream(op: suspend (PostgresDriver.ChangeStream) -> T): T {
        while (true) {
            try {
                val stream = when (val c = conn) {
                    is Conn.Open -> c.stream

                    Conn.Down -> opener(presentedLsn).also { conn = Conn.Open(it, timeSource.markNow()) }
                }

                return op(stream)
            } catch (e: PSQLException) {
                when (val c = conn) {
                    // a stream we failed to release is one whose slot we are about to contend with
                    // ourselves, so it rides along rather than replacing what broke the stream
                    is Conn.Open -> {
                        try { c.stream.close() } catch (t: Throwable) { e.addSuppressed(t) }
                        if (c.openedAt.elapsedNow() >= policy.resetAfter) failures = 0
                    }

                    Conn.Down -> Unit
                }

                conn = Conn.Down
                currentCoroutineContext().ensureActive()

                failures++

                val wait = policy.delayAfter(failures)
                LOG.warn(e, "[$dbName] Replication stream failed (failure $failures since it last made progress); reopening from LSN ${LogSequenceNumber.valueOf(presentedLsn)} in $wait")
                delay(wait)
            }
        }
    }

    private suspend fun ensureConnected() = withStream { }

    override suspend fun poll(): PostgresDriver.Transaction? {
        while (true) {
            val tx = withStream { it.poll() } ?: return null
            failures = 0

            if (tx.lsn > presentedLsn) {
                presentedLsn = tx.lsn
                return tx
            }
        }
    }

    override val walEnd: Long
        get() = when (val c = conn) {
            is Conn.Open -> c.stream.walEnd
            Conn.Down -> presentedLsn
        }

    override val connected: Boolean get() = conn is Conn.Open

    override suspend fun acknowledge(lsn: Long) = withStream { it.acknowledge(lsn) }

    override fun close() =
        when (val c = conn) {
            is Conn.Open -> c.stream.close()
            Conn.Down -> Unit
        }
}
