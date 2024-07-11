package xtdb.api.metrics

import com.sun.net.httpserver.HttpServer
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import xtdb.api.module.XtdbModule
import java.io.IOException
import java.lang.System.Logger.Level.INFO
import java.lang.System.LoggerFinder
import java.net.InetSocketAddress

private val LOGGER = LoggerFactory.getLogger(PrometheusMetrics::class.java)

class PrometheusMetrics(override val registry: PrometheusMeterRegistry, private val server: HttpServer) : Metrics {

    @Serializable
    @SerialName("!Prometheus")
    data class Factory(
        @Serializable val port: Int = 8080
    ): Metrics.Factory {
        override fun openMetrics(): Metrics {
            val reg = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
            try {
                val server = HttpServer.create(InetSocketAddress(port), 0).apply {
                    createContext("/metrics") { exchange ->
                        val resp = reg.scrape().encodeToByteArray()
                        exchange.sendResponseHeaders(200, resp.size.toLong())
                        exchange.responseBody.use { it.write(resp) }
                    }

                    start()
                    LOGGER.info("Prometheus server started on port $port")
                }

                return PrometheusMetrics(reg, server)
            } catch (e: IOException) {
                throw RuntimeException("Failed to start Prometheus server on port $port", e)
            }
        }
    }

    override fun close() {
        server.stop(0)
    }

    /**
     * @suppress
     */
    class Registration : XtdbModule.Registration {
        override fun register(registry: XtdbModule.Registry) {
            registry.registerMetricsFactory(Factory::class)
        }
    }
}