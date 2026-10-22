package xtdb.mysql

import java.io.InputStream
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.thread

/**
 * A TCP proxy in front of the test upstream that can go silent without closing anything — a half-open
 * connection, as when a NAT or a dead host drops traffic without a FIN.
 */
class FreezableProxy(private val targetHost: String, private val targetPort: Int) : AutoCloseable {
    private val server = ServerSocket(0)
    private val sockets = CopyOnWriteArrayList<Socket>()

    @Volatile var frozen = false

    val port: Int get() = server.localPort

    init {
        thread(isDaemon = true, name = "freezable-proxy") {
            while (!server.isClosed) {
                val client = runCatching { server.accept() }.getOrNull() ?: break
                val upstream = Socket(targetHost, targetPort)
                sockets += client; sockets += upstream
                pump(client.getInputStream(), upstream.getOutputStream())
                pump(upstream.getInputStream(), client.getOutputStream())
            }
        }
    }

    private fun pump(from: InputStream, to: OutputStream) =
        thread(isDaemon = true) {
            val buf = ByteArray(16384)
            runCatching {
                while (true) {
                    val n = from.read(buf)
                    if (n < 0) break
                    while (frozen) Thread.sleep(50)
                    to.write(buf, 0, n); to.flush()
                }
            }
        }

    override fun close() {
        server.close()
        sockets.forEach { runCatching { it.close() } }
    }
}
