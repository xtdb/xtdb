package docs

// tag::imports[]
import crux.api.Crux
// end::imports[]

// tag::main[]
fun main() {
    Crux.startNode().use {
        it.submitTx()
    }
}
// end::main[]