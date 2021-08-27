package xtdb.docs

// tag::imports[]
import xtdb.api.IXtdb
// end::imports[]

// tag::main[]
fun main() {
    IXtdb.startNode().use {
        // ...
    }
}
// end::main[]
