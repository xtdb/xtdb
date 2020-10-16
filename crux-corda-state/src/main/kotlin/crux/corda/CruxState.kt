package crux.corda

@Suppress("unused")
interface CruxState {
    val cruxId: Any
    val cruxDoc: Map<String, Any>
}