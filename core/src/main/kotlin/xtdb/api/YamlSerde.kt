@file:JvmName("YamlSerde")

package xtdb.api

import com.charleskorn.kaml.Yaml
import com.charleskorn.kaml.YamlList
import com.charleskorn.kaml.YamlMap
import com.charleskorn.kaml.YamlNode
import com.charleskorn.kaml.YamlNull
import com.charleskorn.kaml.YamlScalar
import com.charleskorn.kaml.YamlTaggedNode
import com.charleskorn.kaml.yamlScalar
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import xtdb.api.log.Log
import xtdb.api.module.XtdbModule
import xtdb.api.storage.ObjectStore
import xtdb.database.ExternalSource
import java.net.InetAddress
import java.nio.file.Path
import java.nio.file.Paths

internal object EnvironmentVariableProvider {
    fun getEnvVariable(name: String): String? = System.getenv(name)
}

// Substitutes `!Env VAR` scalars with their resolved environment variable value.
// Leaves other tags (polymorphic discriminators like `!Local`, `!Kafka`) untouched.
private fun resolveEnvVars(node: YamlNode): YamlNode = when (node) {
    is YamlScalar, is YamlNull -> node
    is YamlList -> node.copy(items = node.items.map(::resolveEnvVars))
    is YamlMap -> node.copy(entries = node.entries.mapValues { (_, v) -> resolveEnvVars(v) })
    is YamlTaggedNode ->
        if (node.tag == "!Env") {
            val varName = node.innerNode.yamlScalar.content
            val value = EnvironmentVariableProvider.getEnvVariable(varName)
                ?: throw IllegalArgumentException("Environment variable '$varName' not found")
            YamlScalar(value, node.innerNode.path)
        } else {
            node.copy(innerNode = resolveEnvVars(node.innerNode))
        }
}

/**
 * @suppress
 */
object PathSerde : KSerializer<Path> {
    override val descriptor = PrimitiveSerialDescriptor("Path", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: Path) {
        throw UnsupportedOperationException("YAML serialization of config is not supported.")
    }

    override fun deserialize(decoder: Decoder): Path = Paths.get(decoder.decodeString())
}

object InetAddressSerde : KSerializer<InetAddress?> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("InetAddress", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: InetAddress?) =
        throw UnsupportedOperationException("serialization not supported for InetAddress")

    override fun deserialize(decoder: Decoder): InetAddress? {
        val host = decoder.decodeString()
        return if (host == "*") null else InetAddress.getByName(host)
    }
}

/**
 * @suppress
 */
val YAML_SERDE = Yaml(
    serializersModule = SerializersModule {
        include(Log.Factory.serializersModule)
        include(Log.Cluster.Factory.serializersModule)
        include(Remote.Factory.serializersModule)
        include(ObjectStore.Factory.serializersModule)
        include(XtdbModule.Factory.serializersModule)
        include(ExternalSource.Factory.serializersModule)
    })

/**
 * @suppress
 */
fun nodeConfig(yamlString: String): Xtdb.Config {
    val root = resolveEnvVars(YAML_SERDE.parseToYamlNode(yamlString))
    return YAML_SERDE.decodeFromYamlNode(root)
}
