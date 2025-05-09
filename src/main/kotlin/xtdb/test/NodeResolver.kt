package xtdb.test

import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolver
import xtdb.api.Xtdb

class NodeResolver : ParameterResolver, AfterEachCallback {

    override fun supportsParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext) =
        parameterContext.parameter.type.isAssignableFrom(Xtdb::class.java)

    private val nodes = mutableMapOf<ExtensionContext, Xtdb>()

    override fun resolveParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext) =
        nodes.computeIfAbsent(extensionContext) { Xtdb.openNode() }

    override fun afterEach(context: ExtensionContext) {
        nodes.remove(context)?.close()
    }
}