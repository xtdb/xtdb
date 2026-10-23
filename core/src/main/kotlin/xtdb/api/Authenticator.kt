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
import java.net.URL
import java.security.MessageDigest
import xtdb.api.error.Forbidden
import xtdb.api.error.Unsupported
import java.time.InstantSource

/**
 * An authenticated user, as returned by [Xtdb.authenticateWithPassword] and its siblings.
 * [Xtdb.connect] opens connections as one, attributing their writes to [userId].
 */
interface Principal {
    val userId: String

    /**
     * The principal with refreshed credentials, or null while these are still valid.
     * @throws Forbidden (`xtdb.authn/refresh-failed`) if the credentials can no longer be refreshed.
     */
    fun revalidate(): Principal?

    data class Static(override val userId: String) : Principal {
        override fun revalidate(): Principal? = null
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
        companion object {
            // Rules are matched in order; the first whose constraints all hold wins.
            // A null constraint matches anything. Null result => no rule matched => reject the connection.
            @JvmStatic
            fun methodFor(rules: List<MethodRule>, user: String?, remoteAddress: String?): Method? =
                rules.firstOrNull { rule ->
                    (rule.user == null || rule.user == user)
                            && (rule.remoteAddress == null || rule.remoteAddress == remoteAddress)
                }?.method
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
