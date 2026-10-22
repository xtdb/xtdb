package xtdb.mysql

import xtdb.util.ReconnectPolicy
import kotlin.time.TimeSource

/**
 * A [MySqlDriver] wrapping another, handing out change streams that survive an interruption
 * of the binlog connection — see [ReconnectingStream].
 *
 * Owns the driver it wraps: closing this closes that one.
 *
 * @suppress
 */
class ResilientDriver(
    private val dbName: String,
    private val driver: MySqlDriver,
    private val policy: ReconnectPolicy = ReconnectPolicy(),
    private val timeSource: TimeSource = TimeSource.Monotonic,
) : MySqlDriver by driver {

    override suspend fun openStream(from: GtidSet): MySqlDriver.ChangeStream =
        ReconnectingStream.open(dbName, driver::openStream, from, policy, timeSource)
}
