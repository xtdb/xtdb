package xtdb.corda.state

@Suppress("unused")
interface XtdbState {
    val xtdbId: Any
    val xtdbDoc: Map<String, Any>
}
