@file:UseSerializers(UrlSerializer::class)

package xtdb.api

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.UseSerializers
import xtdb.UrlSerializer
import xtdb.api.Authenticator.Method.PASSWORD
import xtdb.api.Authenticator.Method.TRUST
import xtdb.api.Authenticator.MethodRule
import org.apache.arrow.memory.BufferAllocator
import xtdb.database.Database
import xtdb.query.IQuerySource
import xtdb.util.requiringResolve
import java.net.InetAddress
import java.net.URL
import java.security.MessageDigest
import xtdb.api.error.Forbidden
import xtdb.api.error.Incorrect
import xtdb.api.error.Unsupported
import java.time.InstantSource

/**
 * An authenticated user, as returned by [Xtdb.authenticateWithPassword] and its siblings.
 * [Xtdb.connect] opens connections as one, attributing their writes to [userId].
 */
interface Principal {
    val userId: String

    /**
     * Refreshes this principal's credentials in place if they've expired; a no-op while they're still valid.
     * Safe to call concurrently from every connection sharing the principal: an expiry is refreshed once.
     *
     * @throws Forbidden (`xtdb.authn/refresh-failed`) if the credentials can no longer be refreshed.
     */
    fun revalidate()

    data class Static(override val userId: String) : Principal {
        override fun revalidate() = Unit
    }
}

val DEFAULT_RULES = listOf(MethodRule(TRUST))

interface Authenticator : AutoCloseable {
    fun methodFor(user: String?, remoteAddress: String?): Method?

    fun verifyPassword(user: String, password: String): Principal =
        throw Unsupported(errorCode="verifyPassword")

    fun startDeviceAuth(user: String): Xtdb.DeviceAuth =
        throw Unsupported(errorCode="startDeviceAuth")

    fun verifyClientCredentials(clientId: String, clientSecret: String): Principal =
        throw Unsupported(errorCode="verifyClientCredentials")

    override fun close() = Unit

    @Serializable
    enum class Method {
        TRUST,
        PASSWORD,
        DEVICE_AUTH,
        CLIENT_CREDENTIALS,
    }

    @Serializable
    data class MethodRule(
        val method: Method,
        val user: String? = null,
        val remoteAddress: String? = null
    ) {
        @Transient
        private val addressBlock = remoteAddress?.let(AddressBlock::parse)

        companion object {
            // Rules are matched in order; the first whose constraints all hold wins.
            // A null constraint matches anything. Null result => no rule matched => reject the connection.
            // An unknown (null) remoteAddress can't be checked against a rule that names one, so it rejects there
            // rather than skipping on to a looser rule further down.
            // A remoteAddress that isn't an IP literal is treated as unknown.
            @JvmStatic
            fun methodFor(rules: List<MethodRule>, user: String?, remoteAddress: String?): Method? {
                val callerAddress = remoteAddress?.let(::parseIpLiteral)
                for (rule in rules) {
                    if (rule.user != null && rule.user != user) continue
                    val block = rule.addressBlock ?: return rule.method
                    if (callerAddress == null) return null
                    if (block.contains(callerAddress)) return rule.method
                }
                return null
            }
        }
    }

    @Serializable
    sealed interface Factory {
        fun open(allocator: BufferAllocator, querySource: IQuerySource, dbCatalog: Database.Catalog): Authenticator

        // The configured users, surfaced through the read-only `pg_user` view.
        // Defaults to the single root user; authenticators with an explicit user set override it.
        fun knownUsers(): List<String> = listOf(SingleRootUserAuthenticator.ROOT_USER)

        // Superuser-only operations gate on this; `pg_user`'s `usesuper` reflects it.
        // On the factory (like `knownUsers`) so info-schema can read it without an init-ordering problem.
        // Defaults to no one; authenticators with a notion of a privileged user override it.
        fun isSuperuser(userId: String): Boolean = false

        // One root user (`xtdb`) whose password is resolved at config-construction time:
        // the explicit YAML/Kotlin value first, then `XTDB_PASSWORD` env var.
        // With no password, connections run TRUST; with a password, PASSWORD is required.
        @Serializable
        @SerialName("!SingleRootUser")
        data class SingleRootUser @JvmOverloads constructor(
            val password: String? = System.getenv("XTDB_PASSWORD"),
        ) : Factory {
            override fun isSuperuser(userId: String) = userId == SingleRootUserAuthenticator.ROOT_USER

            override fun open(allocator: BufferAllocator, querySource: IQuerySource, dbCatalog: Database.Catalog): Authenticator =
                SingleRootUserAuthenticator(password)
        }

        @Serializable
        @SerialName("!OpenIdConnect")
        data class OpenIdConnect @JvmOverloads constructor(
            val issuerUrl: URL,
            val clientId: String,
            val clientSecret: String,
            var rules: List<MethodRule> = DEFAULT_RULES,
            @Transient var instantSource: InstantSource = InstantSource.system()
        ) : Factory {
            fun rules(rules: List<MethodRule>) = apply { this.rules = rules }

            @Suppress("unused")
            fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }

            override fun open(allocator: BufferAllocator, querySource: IQuerySource, dbCatalog: Database.Catalog): Authenticator =
                requiringResolve("xtdb.authn/->oidc-authn")
                    .invoke(this) as Authenticator
        }

        // A fixed set of users with pre-hashed passwords, configured at startup.
        // No mutation path: the only way to change the user set is to edit the config and restart.
        @Serializable
        @SerialName("!UserList")
        data class UserList @JvmOverloads constructor(
            val users: Map<String, PasswordHash>,
            var rules: List<MethodRule> = listOf(MethodRule(PASSWORD)),
        ) : Factory {
            fun rules(rules: List<MethodRule>) = apply { this.rules = rules }

            override fun knownUsers() = users.keys.toList()

            // Same superuser convention as !SingleRootUser: the `xtdb` user, if configured.
            override fun isSuperuser(userId: String) = userId == SingleRootUserAuthenticator.ROOT_USER

            override fun open(allocator: BufferAllocator, querySource: IQuerySource, dbCatalog: Database.Catalog): Authenticator =
                UserListAuthenticator(users, rules)
        }
    }
}

class UserListAuthenticator(
    private val users: Map<String, PasswordHash>,
    private val rules: List<MethodRule>,
) : Authenticator {

    override fun methodFor(user: String?, remoteAddress: String?) =
        MethodRule.methodFor(rules, user, remoteAddress)

    override fun verifyPassword(user: String, password: String): Principal {
        // Unknown user and wrong password fail identically — don't leak which usernames exist.
        if (users[user]?.verify(password) == true) return Principal.Static(user)
        throw Forbidden(errorCode = "xtdb.authn/failed", message = "password authentication failed for user: $user")
    }
}

class SingleRootUserAuthenticator(private val password: String?) : Authenticator {
    companion object {
        const val ROOT_USER = "xtdb"
    }

    override fun methodFor(user: String?, remoteAddress: String?): Authenticator.Method =
        if (password == null) TRUST else PASSWORD

    override fun verifyPassword(user: String, password: String): Principal {
        val configured = this.password
        if (configured != null
            && user == ROOT_USER
            && MessageDigest.isEqual(configured.toByteArray(Charsets.UTF_8), password.toByteArray(Charsets.UTF_8))
        ) {
            return Principal.Static(user)
        }
        throw Forbidden(errorCode = "xtdb.authn/failed", message = "password authentication failed for user: $user")
    }
}

private val IPV4_LITERAL = Regex("""[0-9]{1,3}(\.[0-9]{1,3}){3}""")
private val IPV6_LITERAL = Regex("""[0-9A-Fa-f:][0-9A-Fa-f:.]*:[0-9A-Fa-f:.]*(%[\w.-]+)?""")
private val PREFIX_LENGTH = Regex("""[0-9]{1,3}""")

// InetAddress.getByName resolves anything it doesn't parse as a literal through the name service, and it only
// tries the literal parse when the string starts with a hex digit or a colon - so `x:1` is a DNS lookup.
private fun parseIpLiteral(s: String): InetAddress? =
    if (IPV4_LITERAL.matches(s) || IPV6_LITERAL.matches(s)) runCatching { InetAddress.getByName(s) }.getOrNull()
    else null

private class AddressBlock(private val network: ByteArray, private val prefixLength: Int) {
    fun contains(address: InetAddress): Boolean {
        val bytes = address.address
        if (bytes.size != network.size) return false

        val fullBytes = prefixLength / 8
        for (i in 0 until fullBytes) if (bytes[i] != network[i]) return false

        val remainingBits = prefixLength % 8
        if (remainingBits == 0) return true
        val mask = (0xFF shl (8 - remainingBits)) and 0xFF
        return (bytes[fullBytes].toInt() and mask) == (network[fullBytes].toInt() and mask)
    }

    companion object {
        fun parse(remoteAddress: String): AddressBlock {
            fun malformed(): Nothing = throw Incorrect(
                "remoteAddress must be an IP address or CIDR block: $remoteAddress",
                errorCode = "xtdb.authn/invalid-remote-address",
                data = mapOf("remote-address" to remoteAddress),
            )

            val address = remoteAddress.substringBefore('/')
            // A zone id would be resolved against this host's interfaces, then ignored when matching.
            if ('%' in address) malformed()
            val network = parseIpLiteral(address)?.address ?: malformed()
            val maxPrefix = network.size * 8

            val prefixLength =
                if ('/' !in remoteAddress) maxPrefix
                else remoteAddress.substringAfter('/')
                    .takeIf(PREFIX_LENGTH::matches)?.toInt()?.takeIf { it <= maxPrefix }
                    ?: malformed()

            return AddressBlock(network, prefixLength)
        }
    }
}
