package xtdb.api.metrics

import com.sun.net.httpserver.HttpServer
import io.micrometer.core.instrument.composite.CompositeMeterRegistry
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.InetSocketAddress

private val LOGGER = LoggerFactory.getLogger(PrometheusMetrics::class.java)

class PrometheusMetrics(private val server: HttpServer) : AutoCloseable {

    @Serializable
    data class Factory(val port: Int = 8080) {
        fun open(parentReg: CompositeMeterRegistry): PrometheusMetrics {
            val reg = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
            parentReg.add(reg)

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

                return PrometheusMetrics(server)
            } catch (e: IOException) {
                throw RuntimeException("Failed to start Prometheus server on port $port", e)
            }
        }
    }

    override fun close() {
        server.stop(0)
    }
}
