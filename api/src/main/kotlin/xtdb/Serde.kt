@file:JvmName("Serde")

package xtdb

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import java.time.ZoneId

/**
 * @suppress
 */
object InstantSerde : KSerializer<Instant> {
    override val descriptor = PrimitiveSerialDescriptor("xtdb.instant", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: Instant) = encoder.encodeString(value.toString())
    override fun deserialize(decoder: Decoder): Instant = Instant.parse(decoder.decodeString())
}

/**
 * @suppress
 */
object DurationSerde : KSerializer<Duration> {
    override val descriptor = PrimitiveSerialDescriptor("xtdb.duration", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: Duration) = encoder.encodeString(value.toString())
    override fun deserialize(decoder: Decoder): Duration = Duration.parse(decoder.decodeString())
}

/**
 * @suppress
 */
object ZoneIdSerde : KSerializer<ZoneId> {
    override val descriptor = PrimitiveSerialDescriptor("xtdb.timezone", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: ZoneId) = encoder.encodeString(value.toString())
    override fun deserialize(decoder: Decoder): ZoneId = ZoneId.of(decoder.decodeString())
}

/**
 * @suppress
 */
object PathSerde : KSerializer<Path> {
    override val descriptor = PrimitiveSerialDescriptor("Path", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: Path) { encoder.encodeString(value.toString()) }

    override fun deserialize(decoder: Decoder): Path { return Paths.get(decoder.decodeString()) }
}
