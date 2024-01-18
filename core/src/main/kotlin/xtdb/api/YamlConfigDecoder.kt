@file:JvmName("YamlConfigDecoder")

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
import xtdb.api.storage.InMemoryStorageFactory
import xtdb.api.storage.LocalStorageFactory
import xtdb.api.storage.ObjectStoreFactory
import xtdb.api.storage.StorageFactory
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

fun nodeConfig(yamlString: String): Xtdb.Config =
    YAML_SERDE.decodeFromString<Xtdb.Config>(yamlString)

fun submitClient(yamlString: String): XtdbSubmitClient.Config =
    YAML_SERDE.decodeFromString<XtdbSubmitClient.Config>(yamlString)
