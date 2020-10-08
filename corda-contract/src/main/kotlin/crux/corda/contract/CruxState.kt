package crux.corda.contract

@Suppress("unused")
interface CruxState {
    val cruxId: Any
    val cruxDoc: Map<String, Any>
}