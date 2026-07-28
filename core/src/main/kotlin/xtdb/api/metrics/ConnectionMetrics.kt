package xtdb.api.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer

/**
 * The query and transaction meters an [xtdb.api.Xtdb.Connection] records into.
 *
 * The connection owns them so they fire for every frontend alike — pgwire, in-process ADBC, Flight SQL. They're
 * registered once per node and shared by its connections; a node without a meter registry has none at all, hence
 * the connection's whole [ConnectionMetrics] is nullable rather than each meter within it.
 */
data class ConnectionMetrics(
    val queryTimer: Timer,
    val queryErrorCounter: Counter,
    val txErrorCounter: Counter,
    val txAwaitTimer: Timer,
    val txSubmitTimer: Timer,
    val txExecuteTimer: Timer,
)
