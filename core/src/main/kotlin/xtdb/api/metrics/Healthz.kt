package xtdb.api.metrics

/**
 * A running healthz server. Opened by `xtdb.healthz/open-server`.
 */
interface Healthz : AutoCloseable {

    /** The port the server actually bound to - resolved, so never 0 even when [HealthzConfig.port] is. */
    val port: Int
}
