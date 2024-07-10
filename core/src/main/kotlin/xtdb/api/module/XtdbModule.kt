package xtdb.api.module

import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.serializer
import xtdb.api.IXtdb
import xtdb.api.log.Log
import xtdb.api.metrics.Metrics
import xtdb.api.storage.ObjectStoreFactory
import kotlin.reflect.KClass

interface XtdbModule : AutoCloseable {

    interface Factory{
        val moduleKey: String

        fun openModule(xtdb: IXtdb): XtdbModule
    }

    interface Registry {
        @OptIn(InternalSerializationApi::class)
        fun <F : Factory> registerModuleFactory(factory: KClass<F>, serializer: KSerializer<F> = factory.serializer())

        @OptIn(InternalSerializationApi::class)
        fun <F : Log.Factory> registerLogFactory(factory: KClass<F>, serializer: KSerializer<F> = factory.serializer())

        @OptIn(InternalSerializationApi::class)
        fun <F : ObjectStoreFactory> registerObjectStore(factory: KClass<F>, serializer: KSerializer<F> = factory.serializer())

        @OptIn(InternalSerializationApi::class)
        fun <F : Metrics.Factory> registerMetricsFactory(factory: KClass<F>, serializer: KSerializer<F> = factory.serializer())
    }

    interface Registration {
        fun register(registry: Registry)
    }
}