package xtdb.api.module

import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.serializer
import xtdb.api.IXtdb
import xtdb.api.log.LogFactory
import xtdb.api.storage.ObjectStoreFactory
import kotlin.reflect.KClass

interface Module : AutoCloseable

interface ModuleFactory {
    val moduleKey: String

    fun openModule(xtdb: IXtdb): Module
}

interface ModuleRegistry {
    @OptIn(InternalSerializationApi::class)
    fun <F : ModuleFactory> registerModuleFactory(factory: KClass<F>, serializer: KSerializer<F> = factory.serializer())

    @OptIn(InternalSerializationApi::class)
    fun <F : LogFactory> registerLogFactory(factory: KClass<F>, serializer: KSerializer<F> = factory.serializer())

    @OptIn(InternalSerializationApi::class)
    fun <F : ObjectStoreFactory> registerObjectStore(factory: KClass<F>, serializer: KSerializer<F> = factory.serializer())
}

interface ModuleRegistration {
    fun register(registry: ModuleRegistry)
}
