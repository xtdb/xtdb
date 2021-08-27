package xtdb.docs

// tag::imports[]
import xtdb.api.Crux
// end::imports[]

// tag::main[]
fun main() {
    Crux.startNode().use {
        // ...
    }
}
// end::main[]
