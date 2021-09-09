package xtdb.api

class NodeConfigurationContext private constructor() {
    companion object {
        fun build(block: NodeConfigurationContext.() -> Unit): NodeConfiguration =
            NodeConfigurationContext().also(block).build()
    }

    private val builder = NodeConfiguration.builder()

    operator fun String.invoke(block: ModuleConfigurationContext.() -> Unit) {
        builder.with(this, ModuleConfigurationContext.build(block))
    }

    private fun build() = builder.build()
}
