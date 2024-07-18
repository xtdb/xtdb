package xtdb.arrow

import java.util.*

class Relation (val vectors: SequencedMap<String, Vector>) : AutoCloseable {

    constructor(vectors: List<Vector>)
            : this(vectors.associateByTo(linkedMapOf()) { it.name })

    override fun close() {
        vectors.forEach { (_, vec) -> vec.close() }
    }

    operator fun get(s: String) = vectors[s]
}
