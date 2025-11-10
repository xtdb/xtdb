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

    override val cursorType get() = "pages"
    override val childCursors get() = emptyList<ICursor>()

    private val vals = vals.spliterator()

    override fun tryAdvance(c: Consumer<in List<RelationReader>>) =
        vals.tryAdvance { rows ->
            val rel =
                if (schema == null)
                    Relation.openFromRows(al, rows)
                else
                    Relation(al, schema).closeOnCatch {
                        it.apply { writeRows(*rows.toTypedArray()) }
                    }
            try {
                c.accept(listOf(rel))
            } finally {
                rel.close()
            }
        }
}