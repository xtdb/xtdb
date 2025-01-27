@file:JvmName("YamlSerde")

package xtdb.api

import com.charleskorn.kaml.*
import kotlinx.serialization.KSerializer
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.descriptors.*
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import xtdb.api.log.InMemoryLog
import xtdb.api.log.Log
import xtdb.api.log.LocalLog.Factory
import xtdb.api.module.XtdbModule
import xtdb.api.storage.ObjectStore
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.ServiceLoader.Provider
import kotlin.reflect.KClass

internal object EnvironmentVariableProvider {
    fun getEnvVariable(name: String): String? = System.getenv(name)
}

internal fun envFromTaggedNode(taggedNode: YamlTaggedNode ): String {
    if (taggedNode.tag == "!Env") {
        val value = taggedNode.innerNode.yamlScalar.content
        return EnvironmentVariableProvider.getEnvVariable(value) ?: throw IllegalArgumentException("Environment variable '$value' not found")
    }
    return taggedNode.innerNode.yamlScalar.content
}

internal fun handleEnvTag(input: YamlInput): String {
    val currentLocation = input.getCurrentLocation()
    val scalar = input.node.yamlMap.entries.values.find { it.location == currentLocation }

    return when (scalar) {
        is YamlTaggedNode -> envFromTaggedNode(scalar.yamlTaggedNode)
        is YamlScalar -> scalar.content
        else -> throw IllegalStateException("Expected scalar or tagged node")
    }
}

/**
 * @suppress
 */
object PathWithEnvVarSerde : KSerializer<Path> {
    override val descriptor = PrimitiveSerialDescriptor("PathWithEnvVars", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: Path) { 
        throw UnsupportedOperationException("YAML serialization of config is not supported.")
    }

    override fun deserialize(decoder: Decoder): Path {
        val yamlInput: YamlInput = decoder as YamlInput
        val str = handleEnvTag(yamlInput)
        return Paths.get(str)
    }
}

/**
 * @suppress
 */
object StringWithEnvVarSerde : KSerializer<String> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("StringWithEnvVars", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: String) {
        throw UnsupportedOperationException("YAML serialization of config is not supported.")
    }
    override fun deserialize(decoder: Decoder): String {
        val yamlInput: YamlInput = decoder as YamlInput
        return handleEnvTag(yamlInput)
    }
}

object StringMapWithEnvVarsSerde : KSerializer<Map<String, String>> {
    @OptIn(kotlinx.serialization.ExperimentalSerializationApi::class, kotlinx.serialization.InternalSerializationApi::class)
    override val descriptor: SerialDescriptor = buildSerialDescriptor("StringMapWithEnvVarsSerde", StructureKind.MAP)

    override fun serialize(encoder: Encoder, value: Map<String, String>) {
        throw UnsupportedOperationException("YAML serialization of config is not supported.")
    }

    override fun deserialize(decoder: Decoder): Map<String, String> {
        val yamlInput: YamlInput = decoder as YamlInput
        val currentLocation = yamlInput.getCurrentLocation()
        val mapNode = yamlInput.node.yamlMap.entries.values.find { it.location == currentLocation }?.yamlMap
            ?: throw IllegalStateException("Expected map node at current location")

        return mapNode.entries.entries.associate { (keyNode, valueNode): Map.Entry<YamlScalar, YamlNode> ->
            val key = keyNode.content
            val value = when (valueNode) {
                is YamlTaggedNode -> envFromTaggedNode(valueNode.yamlTaggedNode)
                is YamlScalar -> valueNode.content
                else -> throw IllegalStateException("Expected scalar or tagged node")
            }
            key to value
        }
    }
}

object BooleanWithEnvVarSerde : KSerializer<Boolean> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("BooleanWithEnvVars", PrimitiveKind.BOOLEAN)

    override fun serialize(encoder: Encoder, value: Boolean) {
        throw UnsupportedOperationException("YAML serialization of config is not supported.")
    }
    override fun deserialize(decoder: Decoder): Boolean {
        val yamlInput: YamlInput = decoder as YamlInput
        return handleEnvTag(yamlInput).toBoolean()
    }
}

object IntWithEnvVarSerde : KSerializer<Int> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("BooleanWithEnvVars", PrimitiveKind.BOOLEAN)

    override fun serialize(encoder: Encoder, value: Int) {
        throw UnsupportedOperationException("YAML serialization of config is not supported.")
    }
    override fun deserialize(decoder: Decoder): Int {
        val yamlInput: YamlInput = decoder as YamlInput
        return handleEnvTag(yamlInput).toInt()
    }
}

/**
 * @suppress
 */
val YAML_SERDE = Yaml(
    serializersModule = SerializersModule {
        polymorphic(Log.Factory::class) {
            subclass(InMemoryLog.Factory::class)
            subclass(Factory::class)
        }

        polymorphic(Authenticator.Factory::class) {
            subclass(Authenticator.Factory.UserTable::class)
        }

        ServiceLoader.load(XtdbModule.Registration::class.java)
            .stream()
            .map(Provider<XtdbModule.Registration>::get)
            .forEach {
                it.register(object : XtdbModule.Registry {
                    override fun <F : XtdbModule.Factory> registerModuleFactory(
                        factory: KClass<F>,
                        serializer: KSerializer<F>,
                    ) {
                        polymorphic(XtdbModule.Factory::class) { subclass(factory, serializer) }
                    }

                    override fun <F : Log.Factory> registerLogFactory(
                        factory: KClass<F>,
                        serializer: KSerializer<F>,
                    ) {
                        polymorphic(Log.Factory::class) { subclass(factory, serializer) }
                    }

                    override fun <F : ObjectStore.Factory> registerObjectStore(
                        factory: KClass<F>,
                        serializer: KSerializer<F>,
                    ) {
                        polymorphic(ObjectStore.Factory::class) { subclass(factory, serializer) }
                    }
                })
            }
    })

/**
 * @suppress
 */
fun nodeConfig(yamlString: String): Xtdb.Config =
    YAML_SERDE.decodeFromString<Xtdb.Config>(yamlString)
