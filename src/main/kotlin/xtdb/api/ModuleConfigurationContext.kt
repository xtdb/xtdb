package xtdb.api

import javax.naming.OperationNotSupportedException

class ModuleConfigurationContext private constructor() {
    companion object {
        fun build(block: ModuleConfigurationContext.() -> Unit): ModuleConfiguration =
            ModuleConfigurationContext().also(block).build()
    }

    private val builder = ModuleConfiguration.builder()

    var module: String
        get() = throw OperationNotSupportedException()
        set(value) {
            builder.module(value)
        }

    infix fun String.to(other: Any) {
        builder.set(this, other)
    }

    operator fun String.invoke(block: ModuleConfigurationContext.() -> Unit) {
        builder.with(this, build(block))
    }

    private fun build() = builder.build()
}
