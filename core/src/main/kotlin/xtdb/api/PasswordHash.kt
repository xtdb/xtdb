package xtdb.api

import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.bouncycastle.crypto.generators.Argon2BytesGenerator
import org.bouncycastle.crypto.generators.OpenBSDBCrypt
import org.bouncycastle.crypto.params.Argon2Parameters
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.Base64

/**
 * A stored password hash, tagged in YAML with the algorithm that produced it (`!Argon2id`, `!BCrypt`).
 *
 * The tag — not a sniffed prefix on the string — selects the algorithm, so a new algorithm is a new
 * tag and existing hashes never need re-encoding. Each variant owns the encoding of its [encoded] string
 * and knows how to [verify] a plaintext against it.
 */
@Serializable
sealed interface PasswordHash {
    val encoded: String

    fun verify(password: String): Boolean

    companion object {
        private val secureRandom = SecureRandom()

        fun randomSalt(length: Int): ByteArray = ByteArray(length).also(secureRandom::nextBytes)

        @JvmStatic
        fun argon2id(password: String): Argon2id = Argon2id.derive(password)

        @JvmStatic
        fun bcrypt(password: String): BCrypt = BCrypt.derive(password)
    }

    @Serializable(with = Argon2id.Serde::class)
    @SerialName("!Argon2id")
    data class Argon2id(override val encoded: String) : PasswordHash {

        override fun verify(password: String): Boolean {
            val parsed = parse(encoded) ?: return false
            val actual = hash(password.toCharArray(), parsed.params, parsed.hash.size)
            return MessageDigest.isEqual(actual, parsed.hash)
        }

        // PHC string format: $argon2id$v=19$m=<mem>,t=<iters>,p=<par>$<b64-salt>$<b64-hash>
        internal object Serde : KSerializer<Argon2id> {
            override val descriptor = PrimitiveSerialDescriptor("!Argon2id", PrimitiveKind.STRING)
            override fun serialize(encoder: Encoder, value: Argon2id) = encoder.encodeString(value.encoded)
            override fun deserialize(decoder: Decoder) = Argon2id(decoder.decodeString())
        }

        private class Parsed(val params: Argon2Parameters, val hash: ByteArray)

        companion object {
            // OWASP-recommended argon2id parameters (64 MiB, 3 iterations, single lane).
            private const val MEMORY_KB = 65536
            private const val ITERATIONS = 3
            private const val PARALLELISM = 1
            private const val HASH_LENGTH = 32
            private const val SALT_LENGTH = 16

            private val b64Encoder = Base64.getEncoder().withoutPadding()
            private val b64Decoder = Base64.getDecoder()

            private fun paramsBuilder() =
                Argon2Parameters.Builder(Argon2Parameters.ARGON2_id)
                    .withVersion(Argon2Parameters.ARGON2_VERSION_13)
                    .withMemoryAsKB(MEMORY_KB)
                    .withIterations(ITERATIONS)
                    .withParallelism(PARALLELISM)

            private fun hash(password: CharArray, params: Argon2Parameters, length: Int): ByteArray =
                ByteArray(length).also { out ->
                    Argon2BytesGenerator().apply { init(params) }.generateBytes(password, out)
                }

            fun derive(password: String): Argon2id {
                val salt = randomSalt(SALT_LENGTH)
                val params = paramsBuilder().withSalt(salt).build()
                val hash = hash(password.toCharArray(), params, HASH_LENGTH)
                val encoded = "\$argon2id\$v=${Argon2Parameters.ARGON2_VERSION_13}" +
                        "\$m=$MEMORY_KB,t=$ITERATIONS,p=$PARALLELISM" +
                        "\$${b64Encoder.encodeToString(salt)}\$${b64Encoder.encodeToString(hash)}"
                return Argon2id(encoded)
            }

            private fun parse(encoded: String): Parsed? {
                // $argon2id$v=19$m=65536,t=3,p=1$<salt>$<hash>
                val parts = encoded.split('$')
                if (parts.size != 6 || parts[0].isNotEmpty() || parts[1] != "argon2id") return null
                return try {
                    val version = parts[2].removePrefix("v=").toInt()
                    val perf = parts[3].split(',').associate { it.split('=').let { (k, v) -> k to v.toInt() } }
                    val salt = b64Decoder.decode(parts[4])
                    val hash = b64Decoder.decode(parts[5])
                    val params = Argon2Parameters.Builder(Argon2Parameters.ARGON2_id)
                        .withVersion(version)
                        .withMemoryAsKB(perf.getValue("m"))
                        .withIterations(perf.getValue("t"))
                        .withParallelism(perf.getValue("p"))
                        .withSalt(salt)
                        .build()
                    Parsed(params, hash)
                } catch (_: Exception) {
                    null
                }
            }
        }
    }

    @Serializable(with = BCrypt.Serde::class)
    @SerialName("!BCrypt")
    data class BCrypt(override val encoded: String) : PasswordHash {

        override fun verify(password: String): Boolean =
            try {
                OpenBSDBCrypt.checkPassword(encoded, password.toCharArray())
            } catch (_: Exception) {
                false
            }

        internal object Serde : KSerializer<BCrypt> {
            override val descriptor = PrimitiveSerialDescriptor("!BCrypt", PrimitiveKind.STRING)
            override fun serialize(encoder: Encoder, value: BCrypt) = encoder.encodeString(value.encoded)
            override fun deserialize(decoder: Decoder) = BCrypt(decoder.decodeString())
        }

        companion object {
            private const val COST = 12
            private const val SALT_LENGTH = 16

            fun derive(password: String): BCrypt =
                BCrypt(OpenBSDBCrypt.generate(password.toCharArray(), randomSalt(SALT_LENGTH), COST))
        }
    }
}
