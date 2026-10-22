package xtdb.mysql

import xtdb.util.ReconnectPolicy
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import xtdb.api.error.Unavailable
import xtdb.util.logger
import xtdb.util.warn
import kotlin.time.TimeMark
import kotlin.time.TimeSource

private val LOG = ReconnectingStream::class.logger

/**
 * A [MySqlDriver.ChangeStream] that reopens its underlying stream whenever the binlog connection fails,
 * however many times it takes, from the position of the last transaction it presented.
 *
 * Only an [Unavailable] from opening or polling counts as a failure; anything else, and any failure once the
 * calling coroutine is cancelled, propagates.
 *
 * Owns the stream it holds: closing this closes that one.
 *
 * @suppress
 */
class ReconnectingStream private constructor(
    private val dbName: String,
    private val opener: suspend (GtidSet) -> MySqlDriver.ChangeStream,
    private val policy: ReconnectPolicy,
    private val timeSource: TimeSource,
    from: GtidSet,
) : MySqlDriver.ChangeStream {

    companion object {
        suspend fun open(
            dbName: String,
            opener: suspend (GtidSet) -> MySqlDriver.ChangeStream,
            from: GtidSet,
            policy: ReconnectPolicy = ReconnectPolicy(),
            timeSource: TimeSource = TimeSource.Monotonic,
        ) = ReconnectingStream(dbName, opener, policy, timeSource, from).also { it.ensureConnected() }
    }

    private sealed interface Conn {
        data object Down : Conn
        class Open(val stream: MySqlDriver.ChangeStream, val openedAt: TimeMark) : Conn
    }

    @Volatile private var conn: Conn = Conn.Down

    private var presented = from

    private var failures = 0

    private suspend fun <T> withStream(op: suspend (MySqlDriver.ChangeStream) -> T): T {
        while (true) {
            try {
                val stream = when (val c = conn) {
                    is Conn.Open -> c.stream
                    Conn.Down -> opener(presented).also { conn = Conn.Open(it, timeSource.markNow()) }
                }

                return op(stream)
            } catch (e: Unavailable) {
                when (val c = conn) {
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
                LOG.warn(e, "[$dbName] Binlog stream failed (failure $failures since it last made progress); reopening after GTID set '$presented' in $wait")
                delay(wait)
            }
        }
    }

    private suspend fun ensureConnected() = withStream { }

    override suspend fun poll(): MySqlDriver.Transaction.Committed? {
        val tx = withStream { it.poll() } ?: return null
        failures = 0
        presented = tx.position
        return tx
    }

    override val connected: Boolean get() = (conn as? Conn.Open)?.stream?.connected == true

    override fun close() =
        when (val c = conn) {
            is Conn.Open -> c.stream.close()
            Conn.Down -> Unit
        }
}
