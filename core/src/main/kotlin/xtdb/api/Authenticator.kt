@file:UseSerializers(UrlSerializer::class)

package xtdb.api

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import xtdb.UrlSerializer
import xtdb.api.Authenticator.Method.TRUST
import xtdb.api.Authenticator.MethodRule
import xtdb.database.Database
import xtdb.query.IQuerySource
import xtdb.util.requiringResolve
import java.net.URL
import xtdb.error.Unsupported
import java.time.Instant
import java.time.InstantSource

interface AuthResult {
    val userId: String
}

interface OAuthResult : AuthResult {
    val expiresAt: Instant
    fun withExpiry(expiresAt: Instant): OAuthResult
}

data class SimpleResult(
    override val userId: String
) : AuthResult

data class OAuthPasswordResult(
    override val userId: String,
    override var expiresAt: Instant,
    val accessToken: String,
    val refreshToken: String
) : OAuthResult {
    override fun withExpiry(expiresAt: Instant) = copy(expiresAt = expiresAt)
}

data class OAuthClientCredentialsResult(
    override val userId: String,
    override var expiresAt: Instant,
    val accessToken: String,
    val clientId: String,
    val clientSecret: String
) : OAuthResult {
    override fun withExpiry(expiresAt: Instant) = copy(expiresAt = expiresAt)
}

val DEFAULT_RULES = listOf(MethodRule(TRUST))

interface Authenticator : AutoCloseable {
    fun methodFor(user: String?, remoteAddress: String?): Method

    fun verifyPassword(user: String, password: String): AuthResult =
        throw Unsupported(errorCode="verifyPassword")

    interface DeviceAuthResponse {
        val url: URL
        fun await(): OAuthPasswordResult
    }

    fun startDeviceAuth(user: String): DeviceAuthResponse =
        throw Unsupported(errorCode="startDeviceAuth")

    fun verifyClientCredentials(clientId: String, clientSecret: String): OAuthClientCredentialsResult =
        throw Unsupported(errorCode="verifyClientCredentials")

    fun revalidate(authResult: OAuthResult): OAuthResult =
        throw Unsupported(errorCode="revalidate")

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
    )

    @Serializable
    sealed interface Factory {
        var rules: List<MethodRule>

        fun rules(rules: List<MethodRule>) = apply { this.rules = rules }

        fun open(querySource: IQuerySource, dbCatalog: Database.Catalog): Authenticator

        @Serializable
        @SerialName("!UserTable")
        data class UserTable(override var rules: List<MethodRule> = DEFAULT_RULES) : Factory {
            override fun open(querySource: IQuerySource, dbCatalog: Database.Catalog): Authenticator =
                requiringResolve("xtdb.authn/->user-table-authn")
                    .invoke(this, querySource, dbCatalog) as Authenticator
        }

        @Serializable
        @SerialName("!OpenIdConnect")
        data class OpenIdConnect @JvmOverloads constructor(
            val issuerUrl: URL,
            @Serializable(with = StringWithEnvVarSerde::class) val clientId: String,
            @Serializable(with = StringWithEnvVarSerde::class) val clientSecret: String,
            override var rules: List<MethodRule> = DEFAULT_RULES,
            @Transient var instantSource: InstantSource = InstantSource.system()
        ) : Factory {
            @Suppress("unused")
            fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }
        
            override fun open(querySource: IQuerySource, dbCatalog: Database.Catalog): Authenticator =
                requiringResolve("xtdb.authn/->oidc-authn")
                    .invoke(this) as Authenticator
        }
    }
}
