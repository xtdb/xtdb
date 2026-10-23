package xtdb.flight_sql

import io.grpc.Grpc
import io.grpc.Metadata
import io.grpc.ServerStreamTracer
import io.grpc.ServerStreamTracer.ServerCallInfo
import io.grpc.netty.NettyServerBuilder
import org.apache.arrow.flight.CallHeaders
import org.apache.arrow.flight.FlightServer
import java.net.InetSocketAddress
import java.util.function.Consumer

private const val PEER_ADDR_HEADER = "x-xtdb-peer-addr"
private val PEER_ADDR_KEY = Metadata.Key.of(PEER_ADDR_HEADER, Metadata.ASCII_STRING_MARSHALLER)

// arrow-java's server middleware isn't given the call's transport attributes, so the peer address reaches it
// as a header: a stream tracer's `serverCallStarted` runs before Arrow's middleware, on the same `Metadata`.
// Any copy the client sent is dropped first, so a client can't claim an address of its choosing.
private object PeerAddrTracerFactory : ServerStreamTracer.Factory() {
    override fun newServerStreamTracer(fullMethodName: String, headers: Metadata): ServerStreamTracer {
        headers.removeAll(PEER_ADDR_KEY)

        return object : ServerStreamTracer() {
            override fun serverCallStarted(callInfo: ServerCallInfo<*, *>) {
                headers.removeAll(PEER_ADDR_KEY)
                (callInfo.attributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR) as? InetSocketAddress)
                    ?.address?.hostAddress
                    ?.let { headers.put(PEER_ADDR_KEY, it) }
            }
        }
    }
}

internal fun FlightServer.Builder.withPeerAddrTracer(): FlightServer.Builder =
    transportHint("grpc.builderConsumer", Consumer<NettyServerBuilder> { it.addStreamTracerFactory(PeerAddrTracerFactory) })

/**
 * The caller's address in the form pgwire matches `remoteAddress` rules against (`InetAddress.getHostAddress`),
 * or null where the transport has none.
 */
internal val CallHeaders.peerAddress: String? get() = get(PEER_ADDR_HEADER)
