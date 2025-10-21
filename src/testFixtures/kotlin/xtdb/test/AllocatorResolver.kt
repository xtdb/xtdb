package xtdb.test

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolver

class AllocatorResolver : ParameterResolver, AfterEachCallback {

    override fun supportsParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext) =
        parameterContext.parameter.type.isAssignableFrom(BufferAllocator::class.java)

    private val allocators = mutableMapOf<ExtensionContext, BufferAllocator>()

    override fun resolveParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext) =
        allocators.computeIfAbsent(extensionContext) { RootAllocator() }

    override fun afterEach(context: ExtensionContext) {
        allocators.remove(context)?.close()
    }
}