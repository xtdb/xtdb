package crux.api

object CruxK {
    fun startNode(): ICruxAPI = Crux.startNode()

    fun startNode(block: NodeConfigurationContext.() -> Unit): ICruxAPI =
        Crux.startNode(NodeConfigurationContext.build(block))
}