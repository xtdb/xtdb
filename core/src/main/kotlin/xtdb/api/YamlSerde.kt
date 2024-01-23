@file:JvmName("YamlSerde")

package xtdb.api

import com.charleskorn.kaml.Yaml
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import kotlinx.serialization.serializer
import xtdb.api.log.InMemoryLogFactory
import xtdb.api.log.LocalLogFactory
import xtdb.api.log.LogFactory
import xtdb.api.storage.ObjectStoreFactory
import java.util.ServiceLoader
import java.util.ServiceLoader.Provider
import kotlin.reflect.KClass

interface ModuleRegistry {
    @OptIn(InternalSerializationApi::class)
    fun <F : Xtdb.ModuleFactory> registerModuleFactory(factory: KClass<F>, serializer: KSerializer<F> = factory.serializer())
    @OptIn(InternalSerializationApi::class)
    fun <F : LogFactory > registerLogFactory(factory: KClass<F>, serializer: KSerializer<F> = factory.serializer())
    @OptIn(InternalSerializationApi::class)
    fun <F : ObjectStoreFactory > registerObjectStore(factory: KClass<F>, serializer: KSerializer<F> = factory.serializer())
}

interface ModuleRegistration {
    fun register(registry: ModuleRegistry)
}

val YAML_SERDE = Yaml(
    serializersModule = SerializersModule {
        polymorphic(LogFactory::class) {
            subclass(InMemoryLogFactory::class)
            subclass(LocalLogFactory::class)
        }

        ServiceLoader.load(ModuleRegistration::class.java)
            .stream()
            .map(Provider<ModuleRegistration>::get)
            .forEach {
                it.register(object : ModuleRegistry {
                    override fun <F : Xtdb.ModuleFactory> registerModuleFactory(
                        factory: KClass<F>,
                        serializer: KSerializer<F>,
                    ) {
                        polymorphic(Xtdb.ModuleFactory::class) { subclass(factory, serializer) }
                    }

                    override fun <F : LogFactory> registerLogFactory(
                        factory: KClass<F>,
                        serializer: KSerializer<F>,
                    ) {
                        polymorphic(LogFactory::class) { subclass(factory, serializer) }
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

fun getEnvVariable(name: String): String? = System.getenv(name)

fun replaceEnvVariables(input: String): String {
    val envVarPattern = Regex("!Env\\s+(\\w+)")
    return envVarPattern.replace(input) { matchResult ->
        val envVarName = matchResult.groupValues[1]
        getEnvVariable(envVarName) ?: "null"
    }
}
fun nodeConfig(yamlString: String): Xtdb.Config {
    val yamlWithEnv = replaceEnvVariables(yamlString)
    return YAML_SERDE.decodeFromString<Xtdb.Config>(yamlWithEnv)
}

fun submitClient(yamlString: String): XtdbSubmitClient.Config {
    val yamlWithEnv = replaceEnvVariables(yamlString)
    return YAML_SERDE.decodeFromString<XtdbSubmitClient.Config>(yamlWithEnv)
}
