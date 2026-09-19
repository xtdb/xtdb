package xtdb.postgres

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait

/**
 * A stable address in front of a Postgres container, whose target a test can move.
 *
 * socat runs under a shell loop rather than as the container's own process: killing it is how a connection is
 * dropped, and as PID 1 that would take the container with it.
 */
class PgProxy(network: Network, target: String) : AutoCloseable {

    private val container: GenericContainer<*> =
        GenericContainer("alpine/socat")
            .withNetwork(network)
            .withExposedPorts(PG_PORT)
            .withCreateContainerCmdModifier { it.withEntrypoint("/bin/sh") }
            .withCommand(
                "-c",
                "echo $target > /target; " +
                    "while true; do socat TCP-LISTEN:$PG_PORT,fork,reuseaddr TCP:\$(cat /target):$PG_PORT; sleep 1; done",
            )
            .waitingFor(Wait.forListeningPort())
            .also { it.start() }

    val host: String get() = container.host
    val port: Int get() = container.getMappedPort(PG_PORT)

    /** Moves the target, dropping every connection open through the old one. */
    fun pointAt(target: String) {
        val result = container.execInContainer("/bin/sh", "-c", "echo $target > /target; pkill socat || true")
        check(result.exitCode == 0) { "repointing the proxy failed: ${result.stdout}${result.stderr}" }
    }

    override fun close() {
        runCatching { container.stop() }
    }

    companion object {
        private const val PG_PORT = 5432
    }
}
