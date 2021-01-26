package crux.docs

// tag::imports[]
import crux.api.Crux
// end::imports[]

// tag::main[]
fun main() {
    Crux.startNode().use {
        // ...
    }
}
// end::main[]
