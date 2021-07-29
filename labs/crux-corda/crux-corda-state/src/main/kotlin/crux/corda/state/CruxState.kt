package crux.corda.state

@Suppress("unused")
interface CruxState {
    val cruxId: Any
    val cruxDoc: Map<String, Any>
}