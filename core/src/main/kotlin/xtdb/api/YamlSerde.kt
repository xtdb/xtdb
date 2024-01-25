@file:JvmName("YamlSerde")

package xtdb.api

import com.charleskorn.kaml.*
import kotlinx.serialization.KSerializer
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import xtdb.api.log.Log
import xtdb.api.log.Logs.InMemoryLogFactory
import xtdb.api.log.Logs.LocalLogFactory
import xtdb.api.module.XtdbModule
import xtdb.api.storage.ObjectStoreFactory
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
        else -> throw IllegalStateException()
    }
}

/**
 * @suppress
 */
object PathWithEnvVarSerde : KSerializer<Path> {
    override val descriptor = PrimitiveSerialDescriptor("PathWithEnvVars", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: Path) { encoder.encodeString(value.toString()) }

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
        encoder.encodeString(value)
    }
    override fun deserialize(decoder: Decoder): String {
        val yamlInput: YamlInput = decoder as YamlInput
        return handleEnvTag(yamlInput)
    }
}

/**
 * @suppress
 */
val YAML_SERDE = Yaml(
    serializersModule = SerializersModule {
        polymorphic(Log.Factory::class) {
            subclass(InMemoryLogFactory::class)
            subclass(LocalLogFactory::class)
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

                    override fun <F : ObjectStoreFactory> registerObjectStore(
                        factory: KClass<F>,
                        serializer: KSerializer<F>,
                    ) {
                        polymorphic(ObjectStoreFactory::class) { subclass(factory, serializer) }
                    }
                })
            }
    })

/**
 * @suppress
 */
fun nodeConfig(yamlString: String): Xtdb.Config =
    YAML_SERDE.decodeFromString<Xtdb.Config>(yamlString)

/**
 * @suppress
 */
fun submitClient(yamlString: String): XtdbSubmitClient.Config =
    YAML_SERDE.decodeFromString<XtdbSubmitClient.Config>(yamlString)
