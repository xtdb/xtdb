package xtdb.api

object XtdbKt {
    fun startNode(): IXtdb = IXtdb.startNode()

    fun startNode(block: NodeConfigurationContext.() -> Unit): IXtdb =
        IXtdb.startNode(NodeConfigurationContext.build(block))
}