package xtdb.flight_sql

import org.apache.arrow.flight.CallHeaders
import org.apache.arrow.flight.CallStatus
import org.apache.arrow.flight.FlightRuntimeException
import xtdb.api.Authenticator.Method
import xtdb.api.Principal
import xtdb.api.Xtdb
import xtdb.api.error.Anomaly.Companion.toAnomaly
import xtdb.api.error.Fault
import xtdb.api.error.Unsupported
import xtdb.database.DatabaseName
import java.security.MessageDigest
import java.util.Base64
import javax.crypto.KeyGenerator
import javax.crypto.Mac
import javax.crypto.SecretKey

internal fun unauthenticated(msg: String): FlightRuntimeException =
    CallStatus.UNAUTHENTICATED.withDescription(msg).toRuntimeException()

internal sealed interface Credentials {
    data object None : Credentials

    /** The session id the server returned when it minted the caller's session. */
    class Bearer(val sessionId: String) : Credentials

    sealed class Verifiable(val header: String) : Credentials

    class Basic(header: String, val user: String, val password: String) : Verifiable(header)

    class Client(header: String, val clientId: String, val clientSecret: String) : Verifiable(header)

    companion object {
        private fun decodePair(value: String, scheme: String): Pair<String, String> {
            val decoded = try {
                String(Base64.getDecoder().decode(value), Charsets.UTF_8)
            } catch (_: IllegalArgumentException) {
                throw unauthenticated("malformed $scheme credentials")
            }
            val colon = decoded.indexOf(':')
            if (colon <= 0) throw unauthenticated("malformed $scheme credentials")
            return decoded.substring(0, colon) to decoded.substring(colon + 1)
        }

        fun parse(headers: CallHeaders): Credentials {
            val values = headers.getAll("authorization").toList()
            if (values.size > 1) throw unauthenticated("more than one authorization header")
            val header = values.singleOrNull() ?: return None

            val (scheme, value) = header.trim().split(' ', limit = 2).let { it[0] to it.getOrElse(1) { "" }.trim() }

            return when {
                scheme.equals("Basic", ignoreCase = true) ->
                    decodePair(value, "Basic").let { (user, password) -> Basic(header, user, password) }

                scheme.equals("Client", ignoreCase = true) ->
                    decodePair(value, "Client").let { (id, secret) -> Client(header, id, secret) }

                scheme.equals("Bearer", ignoreCase = true) && value.isNotEmpty() -> Bearer(value)

                else -> throw unauthenticated("unsupported authorization scheme")
            }
        }
    }
}

internal sealed interface Caller {
    fun connect(node: Xtdb, dbName: DatabaseName): Xtdb.Connection

    /** @throws FlightRuntimeException (UNAUTHENTICATED) if the caller's credentials can no longer be refreshed. */
    fun revalidate()

    fun authenticatedBy(credentialMac: ByteArray): Boolean

    data object Anonymous : Caller {
        override fun connect(node: Xtdb, dbName: DatabaseName) = node.connect(dbName)
        override fun revalidate() = Unit
        override fun authenticatedBy(credentialMac: ByteArray) = false
    }

    class Authenticated(private val principal: Principal, private val credentialMac: ByteArray) : Caller {
        override fun connect(node: Xtdb, dbName: DatabaseName) = node.connect(dbName, principal)

        override fun revalidate() = authenticating { principal.revalidate() }

        override fun authenticatedBy(credentialMac: ByteArray) = MessageDigest.isEqual(this.credentialMac, credentialMac)
    }
}

private inline fun <R> authenticating(f: () -> R): R =
    try {
        f()
    } catch (e: FlightRuntimeException) {
        throw e
    } catch (t: Throwable) {
        val anom = t.toAnomaly()
        if (anom is Fault) throw CallStatus.INTERNAL.withDescription("authentication failed").withCause(t).toRuntimeException()
        if (anom is Unsupported) throw CallStatus.UNIMPLEMENTED.withDescription(anom.message).withCause(t).toRuntimeException()
        throw CallStatus.UNAUTHENTICATED.withDescription(anom.message ?: "authentication failed").withCause(t).toRuntimeException()
    }

private fun deviceAuthUnsupported() =
    Unsupported("device authorisation isn't supported over Flight SQL", errorCode = "xtdb.authn/device-auth-unsupported")

private fun noMatchingRule(user: String) = unauthenticated("no authentication record found for user: $user")

internal class FlightAuthn(private val node: Xtdb) {

    private val macKey: SecretKey = KeyGenerator.getInstance("HmacSHA256").generateKey()

    fun mac(credentials: Credentials.Verifiable): ByteArray =
        Mac.getInstance("HmacSHA256").run {
            init(macKey)
            doFinal(credentials.header.toByteArray(Charsets.UTF_8))
        }

    fun authenticate(credentials: Credentials, remoteAddress: String?): Caller = authenticating {
        when (credentials) {
            Credentials.None ->
                if (node.authMethodFor(null, remoteAddress) == Method.TRUST) Caller.Anonymous
                else throw unauthenticated("authentication required")

            is Credentials.Bearer -> throw unauthenticated("unknown session")

            is Credentials.Basic -> {
                val user = credentials.user
                val principal = when (node.authMethodFor(user, remoteAddress)) {
                    Method.TRUST -> node.authenticateTrusted(user)
                    Method.PASSWORD -> node.authenticateWithPassword(user, credentials.password)
                    Method.CLIENT_CREDENTIALS ->
                        throw unauthenticated("send client credentials as `authorization: Client <base64(client-id:client-secret)>`")
                    Method.DEVICE_AUTH -> throw deviceAuthUnsupported()
                    null -> throw noMatchingRule(user)
                }
                Caller.Authenticated(principal, mac(credentials))
            }

            is Credentials.Client -> {
                val clientId = credentials.clientId
                val principal = node.authenticateWithClientCredentials(clientId, credentials.clientSecret)
                // same message as a wrong secret, so a caller the rules turn away can't confirm its secret is valid
                if (node.authMethodFor(principal.userId, remoteAddress) != Method.CLIENT_CREDENTIALS)
                    throw unauthenticated("Client credentials authentication failed for client: $clientId")
                Caller.Authenticated(principal, mac(credentials))
            }
        }
    }
}
