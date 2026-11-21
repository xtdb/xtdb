package xtdb.api.log

import xtdb.index.IndexDef
import java.util.UUID

/** The catalog change a resolved transaction carries, if any, applied by every node that applies the transaction. */
sealed interface Ddl

/** A change to the partition's own secondary indices. */
sealed interface IndexOp : Ddl {
    val name: String

    data class Create(val def: IndexDef) : IndexOp {
        override val name get() = def.name
    }

    /** [id] is the index [name] resolved to, at the resolved head. */
    data class Drop(val id: UUID, override val name: String) : IndexOp
}
