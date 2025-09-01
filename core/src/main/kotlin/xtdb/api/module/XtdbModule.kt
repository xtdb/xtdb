package xtdb.api.module

import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import xtdb.api.Xtdb
import java.util.*

interface XtdbModule : AutoCloseable {

    interface Factory {
        val moduleKey: String

        fun openModule(xtdb: Xtdb): XtdbModule

        companion object {
            val serializersModule = SerializersModule {
                polymorphic(Factory::class) {
                    for (reg in ServiceLoader.load(Registration::class.java))
                        reg.registerSerde(this)
                }
            }
        }
    }

    interface Registration {
        fun registerSerde(builder: PolymorphicModuleBuilder<Factory>)
    }
}