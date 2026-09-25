package xtdb.postgres

import kotlin.time.TimeSource

/**
 * A [PostgresDriver] wrapping another, handing out change streams that survive an interruption
 * of the replication connection — see [ReconnectingStream].
 *
 * Owns the driver it wraps: closing this closes that one.
 *
 * @suppress
 */
class ResilientDriver(
    private val dbName: String,
    private val driver: PostgresDriver,
    private val policy: ReconnectPolicy = ReconnectPolicy(),
    private val timeSource: TimeSource = TimeSource.Monotonic,
) : PostgresDriver by driver {

    override suspend fun openStream(startLsn: Long): PostgresDriver.ChangeStream =
        ReconnectingStream.open(dbName, driver::openStream, startLsn, policy, timeSource)
}
