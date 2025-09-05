package xtdb

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.util.closeOnCatch
import java.util.function.Consumer

class PagesCursor(
    private val al: BufferAllocator,
    private val schema: Schema?,
    vals: Iterable<List<Map<*, *>>>
) : ICursor {

    constructor(al: BufferAllocator, vals: Iterable<List<Map<*, *>>>): this(al, null, vals)

    private val vals = vals.spliterator()

    override fun tryAdvance(c: Consumer<in RelationReader>) =
        vals.tryAdvance { rows ->
            val rel =
                if (schema == null)
                    Relation.openFromRows(al, rows)
                else
                    Relation(al, schema).closeOnCatch {
                        it.apply { writeRows(*rows.toTypedArray()) }
                    }

            rel.use { rel ->
                // TODO won't need openAsRoot call when operators use xtdb.arrow
                rel.openAsRoot(al).use { root -> c.accept(RelationReader.from(root)) }
            }
        }
}