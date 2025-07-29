package xtdb.api.module

import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.serializer
import xtdb.api.Xtdb
import xtdb.api.log.Log
import xtdb.api.storage.ObjectStore
import kotlin.reflect.KClass

interface XtdbModule : AutoCloseable {

    interface Factory {
        val moduleKey: String

        fun openModule(xtdb: Xtdb): XtdbModule
    }

    interface Registry {
        @OptIn(InternalSerializationApi::class)
        fun <F : Factory> registerModuleFactory(factory: KClass<F>, serializer: KSerializer<F> = factory.serializer())

        @OptIn(InternalSerializationApi::class)
        fun <F : Log.Factory> registerLogFactory(factory: KClass<F>, serializer: KSerializer<F> = factory.serializer())

        @OptIn(InternalSerializationApi::class)
        fun <F : Log.Cluster.Factory<*>> registerLogClusterFactory(
            factory: KClass<F>,
            serializer: KSerializer<F> = factory.serializer()
        )

        @OptIn(InternalSerializationApi::class)
        fun <F : ObjectStore.Factory> registerObjectStore(
            factory: KClass<F>,
            serializer: KSerializer<F> = factory.serializer()
        )
    }

    interface Registration {
        fun register(registry: Registry)
    }
}